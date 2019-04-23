#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
from datetime import datetime
import sys
import time
from typing import List
import urllib.parse

from aiohttp import ClientSession
import asyncio
import click
import difflib
from namedlist import namedlist
import xml.etree.ElementTree as ET


@click.command()
@click.argument("obsolescence_chains_csv_file")
@click.option(
    "-m",
    default="cn.dataone.org",
    help="coordinating node: e.g., cn.dataone.org (the default). "
)
@click.option("-n", default=0, help="max number of checks to make")
@click.option("--deep", default=False, is_flag=True, help="check all objects, "
    "not just objects expected to have obsolescence information")
def check_coordinating_node_entries(
    obsolescence_chains_csv_file: str, m: str, n: str, deep: bool
):
    """
    Check obsolescence chains in eml system metadata on DataONE 
    coordinating node against expected values based on the obsolescence 
    chains in ORE objects. The latter are read in from a CSV file 
    obtained by running get_obsolescence_chains.py, followed by 
    resolve_unresolved_dois.py.

Arguments: \n
        OBSOLESCENCE_CHAINS_CSV_FILE (input): obsolescence chains in the form of output from
get_obsolescence_chains.py, followed by resolve_unresolved_dois.py
    """

    # Check the Python version
    if sys.version_info < (3, 7):
        print("Requires Python 3.7 or later")
        exit(0)

    main(obsolescence_chains_csv_file, m, int(n), deep)


UNRESOLVED = "UNRESOLVED"
MAX_RETRIES = 3
BURST_SIZE = 5

metadata_records = collections.OrderedDict()

doi_records = collections.OrderedDict()
DOI_record = namedlist('DOI_record', 
                      'doi obsoletes obsoletedBy metadataPID metadataObsoletesPID metadataObsoletedByPID',
                      default=None)


async def get_metadata(mn: str, pid: str, session: ClientSession, **kwargs) -> str:
    """
    Retrieve system metadata for a package. This is the metadata to which obsolescence
    information may need to be added. Return the response text (the metadata).
    """
    metadata_url = 'https://{}/cn/v2/meta/{}'.format(mn, urllib.parse.quote_plus(pid))
    resp = await session.request(method='GET', url=metadata_url, **kwargs)
    resp.raise_for_status()
    return await resp.text()


async def save_metadata(mn: str, pid: str, session: ClientSession):
    """
    Save the returned metadata in the metadata_records dict. Handle needed retries, if any.
    """
    global metadata_records
    retries = 0
    while retries < MAX_RETRIES:
        try:
            metadata_response = await get_metadata(mn, pid, session)
            break  # no exception, so break out of the retry loop
        except:
            print('Exception: ', sys.exc_info()[0], flush=True)
            retries += 1
            print('retries:', retries, ' ', pid, ' getting metadata', flush=True)
            if retries >= MAX_RETRIES:
                print('Reached max retries getting metadata. Giving up...', flush=True)
                return
            time.sleep(1)
    metadata_records[pid] = metadata_response


async def run_get_metadata_tasks(mn: str, pids: List[str]):
    """
    Fire off a list of tasks to get metadata and save it in the metadata_records
    table.
    """
    async with ClientSession() as session:
        tasks = [save_metadata(mn, pid, session) for pid in pids]
        await asyncio.gather(*tasks)


def check_for_consistency(doi_record, metadata) -> bool:
    """ Check metadata xml for agreement with the DOI record. """
    pid = doi_record.metadataPID
    if not metadata:
        print('Metadata not available for pid', pid, flush=True)
        return False

    expect_obsoletes = doi_record.metadataObsoletesPID
    expect_obsoletedBy = doi_record.metadataObsoletedByPID
    ok = True

    root = ET.fromstring(metadata)
    obsoletes_elements = root.findall('obsoletes')
    if len(obsoletes_elements) > 0:
        have_obsoletes = obsoletes_elements[0].text
    else:
        have_obsoletes = ''
    obsoletedBy_elements = root.findall('obsoletedBy')
    if len(obsoletedBy_elements) > 0:
        have_obsoletedBy = obsoletedBy_elements[0].text
    else:
        have_obsoletedBy = ''

    if expect_obsoletes != have_obsoletes or expect_obsoletedBy != have_obsoletedBy:
        print(pid, flush=True)

    if expect_obsoletes != have_obsoletes:
        print('   Expected obsoletes={}'.format(ascii(expect_obsoletes)), flush=True)
        print('   Found obsoletes   ={}'.format(ascii(have_obsoletes)), flush=True)
        print('\n'.join(difflib.ndiff([expect_obsoletes], [have_obsoletes])))
        ok = False
    if expect_obsoletedBy != have_obsoletedBy:
        print('   Expected obsoletedBy={}'.format(ascii(expect_obsoletedBy)), flush=True)
        print('   Found obsoletedBy   ={}'.format(ascii(have_obsoletedBy)), flush=True)
        print('\n'.join(difflib.ndiff([expect_obsoletedBy], [have_obsoletedBy])))
        ok = False
    if not ok:
        print('\n{}'.format(metadata))
    return ok


def main(obsolescence_chains_csv_file: str, mn: str, max_n: int, deep: bool):

    global doi_records
    global metadata_records
    global sent_count

    # Read in the DOI records
    with open(obsolescence_chains_csv_file, 'r') as input_csv_file:
        # Check for UNRESOLVED entries
        if input_csv_file.read().find(UNRESOLVED) > -1:
            print(
                'UNRESOLVED DOIs found. Please run resolve_unresolved_dois.py '
                'and use its output. Exiting.', flush=True
            )
            exit(0)

        input_csv_file.seek(0)
        csvreader = csv.reader(input_csv_file, delimiter=',')
        # skip the header
        next(csvreader)
        for row in csvreader:
            doi_record = DOI_record(*row)
            if deep or doi_record.metadataObsoletesPID or doi_record.metadataObsoletedByPID:
                pid = doi_record.metadataPID
                doi_records[pid] = doi_record
                # Initialize the metadata_records table, too, so we have rows in the same order. 
                # This will make it easier to check and troubleshoot.
                metadata_records[pid] = None

    print(len(doi_records), flush=True)

    # Go get the metadata that needs to be modified
    print('\nGetting metadata', flush=True)
    count = 0
    pids = []
    for _, doi_record in doi_records.items():
        pids.append(doi_record.metadataPID)
        count += 1
        # Access the member node in bursts so we don't do a denial of service attack on it
        if count % BURST_SIZE == 0:
            # Create tasks to get metadata
            asyncio.run(run_get_metadata_tasks(mn, pids))
            time.sleep(1)
            pids = []
        if count % 1000 == 0:   # Just so we can see signs of life...
            print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)
        if max_n > 0 and count >= max_n:
            break
    asyncio.run(run_get_metadata_tasks(mn, pids))
    print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)

    # Now that we've got the metadata, check it against the expected values
    print('\nChecking metadata', flush=True)
    print(flush=True)

    error_count = 0

    doi_pids = set()
    metadata_pids = set()
    for doi, doi_record in doi_records.items():
        doi_pids.add(doi_record.metadataPID)
    for pid, metadata_record in metadata_records.items():
        metadata_pids.add(pid)
    if doi_pids != metadata_pids:
        print('PIDs that are in DOI table but not in metadata table:', flush=True)
        print(doi_pids - metadata_pids, flush=True)
        print(flush=True)
        print('PIDs that are in metadata table but not in DOI table:', flush=True)
        print(metadata_pids - doi_pids, flush=True)
        print(flush=True)

    for doi, doi_record in doi_records.items():
        pid = doi_record.metadataPID
        if pid in metadata_records:
            metadata_record = metadata_records[pid]
            if metadata_record:
                if not check_for_consistency(doi_record, metadata_record):
                    error_count += 1
    print('\n{} errors found'.format(error_count), flush=True)


if __name__ == '__main__':
    print(datetime.now().strftime('%H:%M:%S'), flush=True)
    try:
        check_coordinating_node_entries()
    finally:
        # click exits via sys.exit(), so we use try/finally to get the
        # ending datetime to display
        print(datetime.now().strftime('%H:%M:%S'), flush=True)
