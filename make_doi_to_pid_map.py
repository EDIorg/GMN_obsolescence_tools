#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
from datetime import datetime
import sys

import click
from namedlist import namedlist


@click.command()
@click.argument("obsolescence_chains_csv_file")
@click.argument("output_tsv_file")
def make_doi_to_pid_map(obsolescence_chains_csv_file: str, output_tsv_file: str):
    """
    Given a CSV file of the form produced by get_obsolescence_chains.py, 
    construct and output a TSV file with rows consisting of a DOI and 
    its corresponding PID. The output TSV can then be used by 
    resolve_unresolved_dois.py when it is run on subsets of the DOIs, 
    avoiding the need to do large numbers of https queries to resolve DOIs.

Arguments: \n
        OBSOLESCENCE_CHAINS_CSV_FILE (input): obsolescence chains as output
        by get_obsolescence_chains.py
        OUTPUT_TSV_FILE (output): name of TSV file to be generated
    """

    # Check the Python version
    if sys.version_info < (3, 7):
        print("Requires Python 3.7 or later")
        exit(0)

    main(obsolescence_chains_csv_file, output_tsv_file)


doi_records = collections.OrderedDict()
DOI_record = namedlist(
    'DOI_record', 
    'doi obsoletes obsoletedBy metadataPID metadataObsoletesPID metadataObsoletedByPID',
    default=None)

doi_to_pid_map = collections.OrderedDict()


def main(obsolescence_chains_csv_file: str, output_tsv_file: str):
    # Read in the DOI records
    with open(obsolescence_chains_csv_file, 'r') as input_csv_file:
        input_csv_file.seek(0)
        csvreader = csv.reader(input_csv_file, delimiter=',')
        # skip the header
        next(csvreader)
        for row in csvreader:
            doi_record = DOI_record(*row)
            if (doi_record.metadataObsoletesPID or doi_record.metadataObsoletedByPID):
                pid = doi_record.metadataPID
                doi_records[pid] = doi_record

    for pid, doi_record in doi_records.items():
        doi_to_pid_map[doi_record.doi] = pid
        if doi_record.obsoletes and doi_record.metadataObsoletesPID:
            doi_to_pid_map[doi_record.obsoletes] = doi_record.metadataObsoletesPID
        if doi_record.obsoletedBy and doi_record.metadataObsoletedByPID:
            doi_to_pid_map[doi_record.obsoletedBy] = doi_record.metadataObsoletedByPID

    # Now write the output
    with open(output_tsv_file, 'w') as output_file:
        columns = [
            'doi', 
            'pid'
        ]
        output_file.write('{}\n'.format(','.join(columns)))
        for doi, pid in doi_to_pid_map.items():
            output_file.write('{},{}\n'.format(doi, pid))


if __name__ == '__main__':
    print(datetime.now().strftime('%H:%M:%S'), flush=True)
    try:
        make_doi_to_pid_map()
    finally:
        # click exits via sys.exit(), so we use try/finally to get the
        # ending datetime to display
        print(datetime.now().strftime('%H:%M:%S'), flush=True)
