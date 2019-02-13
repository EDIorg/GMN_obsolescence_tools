#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import sys

import click
import requests


@click.command()
@click.argument('obsolescence_chains_csv_file')
@click.argument('output_csv_file')
def resolve_unresolved_dois(obsolescence_chains_csv_file: str, output_csv_file: str):
    """
    Updates a CSV file containing the obsolescence chains for DOIs associated with a DataONE Generic Member Node, replacing UNRESOLVED entries in the input CSV file. 

Arguments: \n
        OBSOLESCENCE_CHAINS_CSV_FILE: obsolescence chains as output by get_obsolescence_chains.py \n
        OUTPUT_CSV_FILE: the obsolescence chains CSV file with UNRESOLVED entries resolved
    """
    main(obsolescence_chains_csv_file, output_csv_file)


PASTA_PREFIX = 'https://pasta.lternet.edu/package/metadata/eml/'
UNRESOLVED = 'UNRESOLVED'


def doi2pid(doi: str):
    """
    Get the PID corresponding to the DOI by parsing the landing page
    """
    # Get the html for the landing page corresponding to the doi
    url = "http://dx.doi.org/" + doi
    html = requests.get(url).text
    # Find the PID
    i = html.find('Package ID:')
    j = html.find('<li>', i)
    k = html.find('</li>', j)
    return html[j+len('<li>'):k].replace('&nbsp;', '')


def pid_url(oid):
    """
    Get the PID URL corresponding to the DOI
    """
    pid = doi2pid(oid)
    return PASTA_PREFIX + pid.replace('.', '/')


def main(input_filename: str, output_filename: str):

    unresolved_dois = set()

    _doi = 0
    _obsoletes = 1
    _obsoletedBy = 2
    _metadataPID = 3
    _metadataObsoletesPID = 4
    _metadataObsoletedByPID = 5

    rows = []
    output = []
    # Read in the input CSV file
    with open(input_filename, 'r') as input_file:
        csvreader = csv.reader(input_file, delimiter=',')
        # skip the header
        next(csvreader)
        rows = [row for row in csvreader]

    # Collect all the unresolved dois    
    for doi_record in rows:
        if not doi_record:
            continue
        if doi_record[_metadataPID] == UNRESOLVED:
            unresolved_dois.add(doi_record[_doi])
        if doi_record[_metadataObsoletesPID] == UNRESOLVED:
            unresolved_dois.add(doi_record[_obsoletes])
        if doi_record[_metadataObsoletedByPID] == UNRESOLVED:
            unresolved_dois.add(doi_record[_obsoletedBy])

    # Resolve the unresolved dois
    doi_lookup = {}
    for doi in unresolved_dois:
        doi_lookup[doi] = pid_url(doi)
        print('{} resolves to {}'.format(doi, doi_lookup[doi]))

    # Fill in the resolved pids
    for doi_record in rows:
        if not doi_record:
            continue
        if doi_record[_metadataPID] == UNRESOLVED:
            doi_record[_metadataPID] = doi_lookup[doi_record[_doi]]
        if doi_record[_metadataObsoletesPID] == UNRESOLVED:
           doi_record[_metadataObsoletesPID] = doi_lookup[doi_record[_obsoletes]]
        if doi_record[_metadataObsoletedByPID] == UNRESOLVED:
            doi_record[_metadataObsoletedByPID] = doi_lookup[doi_record[_obsoletedBy]]

    # Now write the output
    with open(output_filename, 'w') as output_file:
        columns = ['doi', 'obsoletes', 'obsoletedBy', 'metadataPID', 'metadataObsoletesPID', 'metadataObsoletedByPID']
        output_file.write('{}\n'.format(','.join(columns)))
        for doi_record in rows:
            if not doi_record:
                continue
            output_file.write('{}\n'.format(','.join(doi_record)))

if __name__ == '__main__':
    resolve_unresolved_dois()
