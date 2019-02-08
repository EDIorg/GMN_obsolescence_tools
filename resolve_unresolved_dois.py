#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import sys

import click


@click.command()
@click.argument('obsolescence_chains_csv_file')
@click.argument('unresolved_dois_file')
@click.argument('output_csv_file')
def resolve_unresolved_dois(obsolescence_chains_csv_file: str, unresolved_dois_file: str, output_csv_file: str):
    """
    Updates a CSV file containing the obsolescence chains for DOIs associated with a DataONE Generic Member Node, replacing UNRESOLVED entries in the input CSV file. 

Arguments: \n
        OBSOLESCENCE_CHAINS_CSV_FILE: obsolescence chains as output by get_obsolescence_chains.py \n
        UNRESOLVED_DOIS_FILE: CSV file with rows consisting of a DOI and the corresponding PASTA Package ID \n
        OUTPUT_CSV_FILE: the obsolescence chain CSV file with UNRESOLVED entries updated based on the UNRESOLVED_DOIS_FILE
    """
    main(obsolescence_chains_csv_file, unresolved_dois_file, output_csv_file)


def main(input_filename: str, unresolved_dois_filename: str,output_filename: str):

    unresolved_dois = {}
    with open(unresolved_dois_filename, 'r') as unresolved_dois_file:
        csvreader = csv.reader(unresolved_dois_file, delimiter=',')
        for row in csvreader:
            if not row:
                continue
            doi = row[0]
            identifier = row[1]
            unresolved_dois[doi] = identifier

    _doi = 0
    _obsoletes = 1
    _obsoletedBy = 2
    _metadataPID = 3
    _metadataObsoletesPID = 4
    _metadataObsoletedByPID = 5

    output = []
    header = True
    with open(input_filename, 'r') as input_file:
        csvreader = csv.reader(input_file, delimiter=',')
        for doi_record in csvreader:
            if not doi_record:
                continue
            if header:
                header = False
                continue
            if doi_record[_doi] in unresolved_dois:
                doi_record[_metadataPID] = unresolved_dois[doi_record[_doi]]
            if doi_record[_obsoletes] in unresolved_dois:
                doi_record[_metadataObsoletesPID] = unresolved_dois[doi_record[_obsoletes]]
            if doi_record[_obsoletedBy] in unresolved_dois:
                doi_record[_metadataObsoletedByPID] = unresolved_dois[doi_record[_obsoletedBy]]
            output.append(doi_record)

    with open(output_filename, 'w') as output_file:
        columns = ['doi', 'obsoletes', 'obsoletedBy', 'metadataPID', 'metadataObsoletesPID', 'metadataObsoletedByPID']
        output_file.write('{}\n'.format(','.join(columns)))
        for output_record in output:
            output_file.write('{0}\n'.format(','.join(output_record)))

if __name__ == '__main__':
    resolve_unresolved_dois()
