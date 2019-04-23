#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
from datetime import datetime
from enum import Enum
import sys
import time
from typing import List
import urllib.parse

from aiohttp import ClientSession
import asyncio
import click
from namedlist import namedlist
from requests import Request, Session
import xml.etree.ElementTree as ET


@click.command()
@click.argument("obsolescence_chains_csv_file")
@click.argument("client_certificate_path")
@click.option(
    "-m",
    default="gmn.lternet.edu",
    help="member node: e.g., gmn.lternet.edu, gmn.edirepository.org. "
         "default: gmn.lternet.edu",
)
@click.option("-n", default=0, help="max number of updates to make")
@click.option(
    "-o",
    default=None,
    help="output TSV file of PIDs and url-encoded metadata for updates made",
)
def update_obsolescence_chains(
    obsolescence_chains_csv_file: str, client_certificate_path: str, m: str, n: str, o: str
):
    """
    Update obsolescence chains in eml system metadata for data packages
    associated with a DataONE Generic Member Node. Optionally outputs a
    TSV file with the updated metadata. Metadata is updated only where
    necessary, i.e., where obsolesence information is expected and the 
    existing metadata has incorrect or missing obsolescence information.

Arguments: \n
        OBSOLESCENCE_CHAINS_CSV_FILE (input): obsolescence chains as output
        by get_obsolescence_chains.py and resolve_unresolved_dois.py
        CLIENT_CERT_PATH (input): path to the X.509 client certificate 
    """

    # Check the Python version
    if sys.version_info < (3, 7):
        print("Requires Python 3.7 or later")
        exit(0)

    main(obsolescence_chains_csv_file, client_certificate_path, m, int(n), o)


UNRESOLVED = "UNRESOLVED"
MAX_RETRIES = 3


metadata_records = collections.OrderedDict()
Metadata_record = namedlist(
    'Metadata_record',
    'pid obsoletes_status obsoletedBy_status metadata original_metadata',
    default=None)

doi_records = collections.OrderedDict()
DOI_record = namedlist(
    'DOI_record', 
    'doi obsoletes obsoletedBy metadataPID metadataObsoletesPID metadataObsoletedByPID',
    default=None)


class Tag_Status(Enum):
    UNKNOWN = 0
    OK = 1
    ADD = 2
    REPLACE = 3
    REMOVE = 4


async def get_metadata(mn: str, pid: str, session: ClientSession, **kwargs) -> str:
    """
    Retrieve system metadata for a package. This is the metadata to which obsolescence
    information may need to be added. Return the response text (the metadata).
    """
    metadata_url = 'https://{}/mn/v2/meta/{}'.format(mn, urllib.parse.quote_plus(pid))
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
            print('retries:', retries, ' ', pid, '  getting metadata', flush=True)
            if retries >= MAX_RETRIES:
                print('Reached max retries getting metadata. Giving up...', flush=True)
                return
            time.sleep(1)
    metadata_records[pid].original_metadata = metadata_response


async def run_get_metadata_tasks(mn: str, pids: List[str]):
    """
    Fire off a list of tasks to get metadata and save it in the
    metadata_records table.
    """
    async with ClientSession() as session:
        tasks = [save_metadata(mn, pid, session) for pid in pids]
        await asyncio.gather(*tasks)


def add_tag(root, tag, text, after_tags=None):
    """
    Adds an XML tag, placing. Returns the root of the updated element tree.

    after_tags is a sequence of tags. The new tag is added after the
    first tag in the list that is actually present.
    """
    index = -1
    if after_tags:
        for after_tag in after_tags:
            if len(root.findall('.//' + after_tag)) > 0:
                children = [child.tag for child in root]
                index = children.index(after_tag) + 1
                break
    if index > -1:
        node = ET.Element(tag)
        node.text = text
        root.insert(index, node)
    return root


def replace_tag(root, tag, text):
    """
    Replaces the text value for an XML tag.
    Returns the root of the updated element tree.
    """
    node = root.find(tag)
    node.text = text
    return root


def remove_tag(root, tag):
    """
    Removes an XML tag.
    Returns the root of the updated element tree.
    """
    node = root.find(tag)
    root.remove(node)
    return root


def status_text(tag_status):
    """
    Format the status for saving in the output TSV file
    """
    if tag_status == Tag_Status.OK:
        return ""
    elif tag_status == Tag_Status.ADD:
        return "ADD"
    elif tag_status == Tag_Status.REPLACE:
        return "REPLACE"
    elif tag_status == Tag_Status.REMOVE:
        return "REMOVE"


sent_count = 0


def send_update_sys_metadata(mn: str, pid: str, metadata_xml: str, client_certificate_path: str):
    """
    Send a updateSysMetadata request to the member node.
    This is not done asynchronously because we need to use PreparedRequest.
    """
    print('updateSysMetadata: ', pid, flush=True)
    global sent_count
    sent_count += 1
    session = Session()    
    # To get a multipart request in the needed format, we need to do 
    #   some fiddling...
    *_, scope, identifier, revision = pid.split('/')
    # We're not really going to use an xml file, but we provide a name
    #   to make log entries clearer
    sysmeta_filename = '.'.join([scope, identifier, revision]) + '.sysmeta.xml'
    prepped_request = Request(
        'PUT', 
        'https://{}/mn/v2/meta'.format(mn),
        files={
            'pid': (None, pid),
            'sysmeta': (sysmeta_filename, metadata_xml.encode('ascii'), 'application/xml')
        }).prepare()
    retries = 0
    status_code = None
    while retries < MAX_RETRIES:
        try:
            resp = session.send(prepped_request, cert=client_certificate_path)
            if resp.status_code != 200:
                print('{} return status code = {}'.format(sysmeta_filename, str(resp.status_code)), flush=True)
                retries += 1
                if retries >= MAX_RETRIES:
                    print('Reached max retries updating metadata. Giving up...', flush=True)
                    print(metadata_xml, flush=True)
                    return
                time.sleep(1)
            else:
                break
        except:
            print('Exception: ', sys.exc_info(), flush=True)
            retries += 1
            print('retries:', retries, ' ', pid, '  getting metadata', flush=True)
            if retries >= MAX_RETRIES:
                print('Reached max retries updating metadata. Giving up...', flush=True)
                return
            time.sleep(1)
    return status_code


def fixup_metadata_xml(mn, pid, client_certificate_path):
    """
    Given the current system metadata for a package and the desired
    obsolescence information,
       - determines if the system metadata needs to be modified
       - if so, generates the updated system metadata and updates it
    on the member node
    """
    global metadata_records

    original_metadata = metadata_records[pid].original_metadata
    obsoletes = doi_records[pid].metadataObsoletesPID
    obsoletedBy = doi_records[pid].metadataObsoletedByPID

    root = ET.fromstring(original_metadata)

    # Tentative status, which may be superseded below
    if obsoletes:
        obsoletes_status = Tag_Status.ADD
    else:
        obsoletes_status = Tag_Status.OK
    if obsoletedBy:
        obsoletedBy_status = Tag_Status.ADD
    else:
        obsoletedBy_status = Tag_Status.OK

    for child in root:

        if child.tag == 'obsoletes':
            obsoletes_value = child.text
            if obsoletes:
                if obsoletes == obsoletes_value:
                    # obsoletes tag is present and has the desired value
                    obsoletes_status = Tag_Status.OK
                else:
                    # obsoletes tag is present but the value isn't what we want
                    obsoletes_status = Tag_Status.REPLACE
            else:
                # obsoletes tag is present but shouldn't be
                obsoletes_status = Tag_Status.REMOVE

        if child.tag == 'obsoletedBy':
            obsoletedBy_value = child.text
            if obsoletedBy:
                if obsoletedBy == obsoletedBy_value:
                    # obsoletedBy tag is present and has the desired value
                    obsoletedBy_status = Tag_Status.OK
                else:
                    # obsoletedBy tag is present but the value isn't what
                    # we want
                    obsoletedBy_status = Tag_Status.REPLACE
            else:
                # obsoletedBy tag is present but shouldn't be
                obsoletedBy_status = Tag_Status.REMOVE

    # If needed, update the metadata

    if obsoletes_status == Tag_Status.ADD:
        root = add_tag(root, 'obsoletes', obsoletes, ('replicationPolicy', 'accessPolicy'))
    elif obsoletes_status == Tag_Status.REPLACE:
        root = replace_tag(root, 'obsoletes', obsoletes)
    elif obsoletes_status == Tag_Status.REMOVE:
        root = remove_tag(root, 'obsoletes')

    # Put the obsoletedBy tag in the right position; otherwise, schema validation fails
    if len(root.findall('obsoletes')) > 0:
        prev = ('obsoletes',)
    else:
        prev = ('replicationPolicy', 'accessPolicy')

    if obsoletedBy_status == Tag_Status.ADD:
        root = add_tag(root, 'obsoletedBy', obsoletedBy, prev)
    elif obsoletedBy_status == Tag_Status.REPLACE:
        root = replace_tag(root, 'obsoletedBy', obsoletedBy)
    elif obsoletedBy_status == Tag_Status.REMOVE:
        root = remove_tag(root, 'obsoletedBy')

    metadata = ET.tostring(root).decode('utf-8')

    if obsoletes_status != Tag_Status.OK or obsoletedBy_status != Tag_Status.OK:
        send_update_sys_metadata(mn, pid, metadata, client_certificate_path)

    metadata_records[pid].obsoletes_status = obsoletes_status
    metadata_records[pid].obsoletedBy_status = obsoletedBy_status
    metadata_records[pid].metadata = metadata

    return (
        status_text(obsoletes_status),
        status_text(obsoletedBy_status),
        metadata,
        original_metadata,
    )


def write_output_tsv(output_tsv_file):
    if not output_tsv_file:
        return
    with open(output_tsv_file, 'w') as output_tsv:
        output_tsv.write(
            # Write the headers
            'pid\tobsoletes\tobsoletedBy\tmetadata\toriginal_metadata\n')
        for pid, metadata_record in metadata_records.items():
            output_tsv.write('{}\t{}\t{}\t{}\t{}\n'.format(
                metadata_record.pid, 
                status_text(metadata_record.obsoletes_status),
                status_text(metadata_record.obsoletedBy_status),
                metadata_record.metadata,
                metadata_record.original_metadata))


def main(obsolescence_chains_csv_file: str,
         client_certificate_path: str,
         mn: str,
         max_n: int,
         output_tsv_file: str):

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
            if (doi_record.metadataObsoletesPID or doi_record.metadataObsoletedByPID):
                pid = doi_record.metadataPID
                doi_records[pid] = doi_record
                # Initialize the metadata_records table, too, so we have rows in the same order. 
                # This will make it easier to check and troubleshoot.
                metadata_records[pid] = Metadata_record(pid, '', '', '', 'NA')

    # Go get the metadata that needs to be modified
    print('Getting metadata', flush=True)
    count = 0
    pids = []
    for _, doi_record in doi_records.items():
        # Has obsolescence info so metadata needs to be modified
        pids.append(doi_record.metadataPID)
        count += 1
        # Access the member node in bursts so we don't do a denial of service attack on it
        if count % 25 == 0:
            # Create tasks to get metadata
            asyncio.run(run_get_metadata_tasks(mn, pids))
            time.sleep(1)
            pids = []
        if count % 1000 == 0:   # Just so we can see signs of life...
            print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)
    asyncio.run(run_get_metadata_tasks(mn, pids))
    print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")), flush=True)

    # Now that we've got the metadata, modify it as needed and update it on the member node
    print('Updating metadata', flush=True)
    pids = []
    for pid, metadata_record in metadata_records.items():
        if metadata_record.original_metadata == 'NA':
            print('Unexpected Error: original_metadata not found for {}'.format(pid), flush=True)
            continue
        count += 1
        fixup_metadata_xml(mn, pid, client_certificate_path)        

    write_output_tsv(output_tsv_file)


if __name__ == '__main__':
    print(datetime.now().strftime('%H:%M:%S'), flush=True)
    try:
        update_obsolescence_chains()
    finally:
        # click exits via sys.exit(), so we use try/finally to get the
        # ending datetime to display
        print(datetime.now().strftime('%H:%M:%S'), flush=True)
