#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import collections
import csv
from datetime import datetime
import sys
import time
from typing import List

from aiohttp import ClientSession
import click
from namedlist import namedlist
import xml.etree.ElementTree as ET


TRACE = False
UNRESOLVED = 'UNRESOLVED'
MAX_RETRIES = 3

doi_records = collections.OrderedDict()

DOI_record = namedlist(
    'DOI_record', 
    'doi obsoletes obsoletedBy metadataPID metadataObsoletesPID metadataObsoletedByPID',
    default=None)


@click.command()
@click.option('-m', 
    default='gmn.lternet.edu', 
    help='member node: e.g., gmn.lternet.edu, gmn.edirepository.org. default: gmn.lternet.edu')
@click.argument('doi_file')
@click.argument('output_csv_file')
def get_obsolescence_chains(m: str, doi_file: str, output_csv_file: str):
    """
    Generates a CSV file containing the obsolescence chains for DOIs associated with a DataONE Generic Member Node. 

Arguments: \n
        DOI_FILE: text file containing a list of DOIs, one per line \n
        OUTPUT_CSV_FILE: the CSV file to be generated 

        The output CSV file will have a header row. Columns are:  \n
            doi, obsoletes, obsoletedBy, metadataPID, metadataPIDObsoletes, metadataPIDObsoletedBy

        If metadata is not available for a DOI, the corresponding metadata PID entries will be UNRESOLVED
    """

    # Check the Python version
    if (sys.version_info < (3, 7)):
        print('Requires Python 3.7 or later')
        exit(0)

    main(m, doi_file, output_csv_file)


async def get_ORE_metadata(mn: str, doi: str, session: ClientSession, **kwargs) -> str:
    metadata_url = 'https://{}/mn/v2/meta/{}'.format(mn, doi)
    resp = await session.request(method='GET', url=metadata_url, **kwargs)
    resp.raise_for_status()
    return await resp.text()


async def get_ORE_object(mn: str, doi: str, session: ClientSession, **kwargs) -> str:
    object_url = 'https://{}/mn/v2/object/{}'.format(mn, doi)
    resp = await session.request(method='GET', url=object_url, **kwargs)
    resp.raise_for_status()
    return await resp.text()


async def parse_ORE_metadata(mn: str, doi: str, session: ClientSession):
    global doi_records
    retries = 0
    while retries < MAX_RETRIES:
        try:
            metadata_response = await get_ORE_metadata(mn, doi, session)
            break  # no exception, so get out of the retry loop
        except:
            print('Exception: ', sys.exc_info()[0])
            retries += 1
            print('retries:', retries, ' ', doi, '  getting ORE metadata')
            if retries >= MAX_RETRIES:
                print('Reached max retries getting ORE metadata. Giving up...')
                return
            time.sleep(1)

    obsoletes = None
    obsoletedBy = None
    metadataObsoletesPID = None
    metadataObsoletedByPID = None

    root = ET.fromstring(metadata_response)
    for child in root:
        if child.tag == 'obsoletes':
            obsoletes = child.text
            metadataObsoletesPID = UNRESOLVED
            if TRACE:
                print('{} obsoletes {}'.format(obsoletes, doi))
        elif child.tag == 'obsoletedBy':
            obsoletedBy = child.text
            metadataObsoletedByPID = UNRESOLVED
            if TRACE:
                print('{} obsoletedBy {}'.format(obsoletedBy, doi))
    if doi not in doi_records:
        print('Unexpected Error - parse_ORE_metadata finds doi not in dictionary: ', doi)
        doi_records[doi] = DOI_record(doi, obsoletes, obsoletedBy, UNRESOLVED, None, None)
    doi_records[doi].obsoletes = obsoletes
    doi_records[doi].obsoletedBy = obsoletedBy        
    doi_records[doi].metadataObsoletesPID = metadataObsoletesPID
    doi_records[doi].metadataObsoletedByPID = metadataObsoletedByPID


async def parse_ORE_object(mn: str, doi: str, session: ClientSession):
    global doi_records

    retries = 0
    while retries < MAX_RETRIES:
        try:
            object_response = await get_ORE_object(mn, doi, session)
            break  # no exception, so get out of the retry loop
        except:
            print('Exception: ', sys.exc_info()[0])
            retries += 1
            print('retries:', retries, ' ', doi, '  getting ORE object')
            if retries >= MAX_RETRIES:
                print('Reached max retries getting ORE object. Giving up...')
                return
            time.sleep(1)    

    metadataPID = None
    matched_lines = [line for line in object_response.split('\n') 
        if "https://pasta.lternet.edu/package/metadata/eml/" in line]
    if len(matched_lines) == 0:
        print('metadataPID not found for {}'.format(doi))
        return
    if len(matched_lines) > 1:
        print('Multiple metadataPIDs found for {}'.format(doi))
        return
    metadataPID = matched_lines[0].strip().replace(
        '<dcterms:identifier>', '').replace('</dcterms:identifier>', '')
    if doi in doi_records:  
        doi_records[doi].metadataPID = metadataPID


async def run_ORE_metadata_tasks(mn: str, dois: List[str]):
    async with ClientSession() as session:
        tasks = [parse_ORE_metadata(mn, doi, session) for doi in dois]
        await asyncio.gather(*tasks)


async def run_ORE_object_tasks(mn: str, dois: List[str]):
    async with ClientSession() as session:
        tasks = [parse_ORE_object(mn, doi, session) for doi in dois]
        await asyncio.gather(*tasks)


def process_doi_file(mn: str, doi_filename: str):
    count = 0
    dois = []
    with open(doi_filename, mode='r') as doi_file:
        for doi in doi_file:
            doi = doi.strip()
            if not doi:
                continue
            dois.append(doi)
            # Create a record for the doi so we'll have a row for it even if http fails
            if doi not in doi_records:
                doi_records[doi] = DOI_record(doi, None, None, UNRESOLVED, None, None)
            else:
                print('Unexpected Error - attempted to add a doi that was already in the dict: ', 
                      doi)
            count += 1
            # Access the member node in bursts so we don't do a denial of service attack on it
            if count % 25 == 0:
                asyncio.run(run_ORE_metadata_tasks(mn, dois))
                time.sleep(1)
                asyncio.run(run_ORE_object_tasks(mn, dois))
                time.sleep(1)
                dois = []
            if count % 1000 == 0:   # Just so we can see signs of life...
                print('count = {}, time = {}'.format(count, datetime.now().strftime("%H:%M:%S")))
    # Pick up the leftover dois, if any
    asyncio.run(run_ORE_metadata_tasks(mn, dois))
    asyncio.run(run_ORE_object_tasks(mn, dois))


def resolve_metadataPIDs():
    for doi, doi_record in doi_records.items():
        if doi in doi_records:
            if doi_record.obsoletes:
                if doi_record.obsoletes in doi_records:                    
                    doi_record.metadataObsoletesPID = doi_records[doi_record.obsoletes].metadataPID
                else:
                    doi_record.metadataObsoletesPID = UNRESOLVED
                    print('doi not resolved: {}'.format(doi_record.obsoletes))
            if doi_record.obsoletedBy:
                if doi_record.obsoletedBy in doi_records:                    
                    doi_record.metadataObsoletedByPID = doi_records[doi_record.obsoletedBy].metadataPID
                else:
                    doi_record.metadataObsoletedByPID = UNRESOLVED
                    print('doi not resolved: {}'.format(doi_record.obsoletedBy))
        else:
            print('doi not found: {}'.format(doi))


def save_to_csv(csv_filename: str):
    columns = [
        'doi', 
        'obsoletes', 
        'obsoletedBy', 
        'metadataPID', 
        'metadataObsoletesPID', 
        'metadataObsoletedByPID']
    with open(csv_filename, mode='w') as obsolescence_csv:
        csv_writer = csv.writer(
            obsolescence_csv, 
            delimiter=',', 
            quotechar='"', 
            quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(columns)
        for doi, doi_record in doi_records.items():
            csv_writer.writerow(list(doi_record))


def main(mn: str, doi_filename: str, csv_filename: str):
    process_doi_file(mn, doi_filename)
    resolve_metadataPIDs()
    save_to_csv(csv_filename)


if __name__ == '__main__':
    print(datetime.now().strftime("%H:%M:%S"))
    try:
        get_obsolescence_chains()
    finally:  
        # click exits via sys.exit(), so we use try/finally to get the ending datetime to display
        print(datetime.now().strftime("%H:%M:%S"))
