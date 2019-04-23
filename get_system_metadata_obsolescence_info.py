#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
from datetime import datetime
import sys
import time
from typing import List
import urllib.parse

from aiohttp import ClientSession
import asyncio
import click
import xml.etree.ElementTree as ET


@click.command()
@click.argument("pids_list_file")
@click.argument("obsolescence_info_csv_file")
@click.option(
    "-d",
    default="gmn.lternet.edu",
    help="node domain: e.g., gmn.lternet.edu, cn.dataone.org. "
         "default: gmn.lternet.edu",
)
@click.option(
    "-t",
    default="mn",
    help="type of node: mn or cn, default=mn"
)
def get_system_metadata_obsolescence_info(
    pids_list_file: str, obsolescence_info_csv_file: str, d: str, t: str
):
    """
    Query a MN or CN to get the system metadata corresponding to a list of PIDs and output the PID, obsoletes, and obsoletedBy in an output CSV file.

Arguments: \n
        PIDS_LIST_FILE (input): a list of PIDs, one per line
        OBSOLESCENCE_INFO_CSV_FILE (output): PID, obsoletes, obsoletedBy
    """

    # Check the Python version
    if sys.version_info < (3, 7):
        print("Requires Python 3.7 or later")
        exit(0)

    main(pids_list_file, obsolescence_info_csv_file, d, t)


MAX_RETRIES = 3
BURST_SIZE = 10


metadata_records = collections.OrderedDict()
output_records = collections.OrderedDict()
failures = collections.OrderedDict()


async def get_metadata(domain: str, node_type: str, pid: str, session: ClientSession, **kwargs) -> str:
    # print('get_metadata', domain, node_type, pid)
    """
    Retrieve system metadata for a package. This is the metadata to which obsolescence
    information may need to be added. Return the response text (the metadata).
    """
    metadata_url = 'https://{}/{}/v2/meta/{}'.format(domain, node_type, urllib.parse.quote_plus(pid))
    # print(metadata_url)
    resp = await session.request(method='GET', url=metadata_url, **kwargs)
    resp.raise_for_status()
    return await resp.text()


async def save_metadata(domain: str, node_type: str, pid: str, session: ClientSession):
    """
    Save the returned metadata in the metadata_records dict. Handle needed retries, if any.
    """
    global metadata_records
    retries = 0
    while retries < MAX_RETRIES:
        try:
            metadata_response = await get_metadata(domain, node_type, pid, session)
            break  # no exception, so break out of the retry loop
        except:
            print('Exception: ', sys.exc_info()[0], flush=True)
            retries += 1
            print('retries:', retries, ' ', pid, '  getting metadata', flush=True)
            if retries >= MAX_RETRIES:
                print('Reached max retries getting metadata. Giving up...', flush=True)
                failures[pid] = sys.exc_info()[0]
                return
            time.sleep(1)
    metadata_records[pid] = metadata_response


async def run_get_metadata_tasks(domain: str, node_type: str, pids: List[str]):
    """
    Fire off a list of tasks to get metadata and save it in the metadata_records
    table.
    """
    async with ClientSession() as session:
        tasks = [save_metadata(domain, node_type, pid, session) for pid in pids]
        await asyncio.gather(*tasks)


def parse_metadata(metadata: str):
    root = ET.fromstring(metadata)
    identifier_elements = root.findall('identifier')
    identifier = identifier_elements[0].text
    obsoletes_elements = root.findall('obsoletes')
    if len(obsoletes_elements) > 0:
        obsoletes = obsoletes_elements[0].text
    else:
        obsoletes = ''
    obsoletedBy_elements = root.findall('obsoletedBy')
    if len(obsoletedBy_elements) > 0:
        obsoletedBy = obsoletedBy_elements[0].text
    else:
        obsoletedBy = ''
    output_records[identifier] = (identifier, obsoletes, obsoletedBy)


def main(pids_list_filename: str, obsolescence_info_csv_filename: str, domain: str, node_type: str):

    global metadata_records

    # Read in the PIDs
    pids_list = []
    with open(pids_list_filename, 'rt') as pids_list_file:
        for line in pids_list_file:
            pids_list.append(line.strip())
    for pid in pids_list:
        output_records[pid] = (pid, '', '')

    print(len(pids_list), flush=True)

    # Go get the metadata
    print('\nGetting metadata', flush=True)
    count = 0
    pids = []
    for pid in pids_list:
        pids.append(pid)
        count += 1
        # Access the member node in bursts so we don't do a denial of service attack on it
        if count % BURST_SIZE == 0:
            # Create tasks to get metadata
            asyncio.run(run_get_metadata_tasks(domain, node_type, pids))
            time.sleep(1)
            pids = []
        if count % 1000 == 0:   # Just so we can see signs of life...
            print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)
    asyncio.run(run_get_metadata_tasks(domain, node_type, pids))
    print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)

    # Now that we've got the metadata, check it against the expected values
    print('\nParsing metadata', flush=True)
    print(flush=True)
    for pid in pids_list:
        if pid in metadata_records:
            parse_metadata(metadata_records[pid])
        else:
            output_records[pid] = (pid, 'FAILED', 'FAILED')

    # Write the output
    with open(obsolescence_info_csv_filename, 'wt') as obsolescence_info_csv_file:
        # Write header
        obsolescence_info_csv_file.write('PID,obsoletes,obsoletedBy\n')
        for pid, obsoletes, obsoletedBy in output_records.values():
            obsolescence_info_csv_file.write('{},{},{}\n'.format(pid, obsoletes, obsoletedBy))


if __name__ == '__main__':
    print(datetime.now().strftime('%H:%M:%S'), flush=True)
    try:
        get_system_metadata_obsolescence_info()
    finally:
        # click exits via sys.exit(), so we use try/finally to get the
        # ending datetime to display
        print(datetime.now().strftime('%H:%M:%S'), flush=True)
