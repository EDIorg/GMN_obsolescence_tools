#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
from typing import List

import click


@click.command()
@click.argument('obsolescence_chain_csv_file')
def check_obsolescence_chains(obsolescence_chain_csv_file: str):
    """
    Checks obsolescence chains for consistency:\n
       - are the revisions in ascending order?\n
       - do obsoletes and obsoletedBy entries match up?

    Input is a CSV file in the format of output from get_obsolescence_chains.py and
    resolve_unresolved_dois.py.
    """

    main(obsolescence_chain_csv_file)


_pid_prefix = 'https://pasta.lternet.edu/package/metadata/eml/'

_doi = 0
_obsoletes = 1
_obsoletedBy = 2
_metadataPID = 3
_metadataObsoletesPID = 4
_metadataObsoletedByPID = 5

_scope = 0
_identifier = 1
_revision = 2


def parse_package_id(package_id: str):
    """
    Takes a package_id in the form 'scope.identifier.revision' and returns a
    triple (scope, identifier, revision) where the identifier and revision are
    ints, suitable for sorting.
    """
    *_, scope, identifier, revision = package_id.split('/')
    return (scope, int(identifier), int(revision))


def _sort_by_package_id(csv_row: List[str]):
    return parse_package_id(csv_row[_metadataPID])


def _read_and_sort_obsolescence_chains_csv_file(chains_filename: str):
    csv_rows = []
    with open(chains_filename, 'r') as chains_file:
        csvreader = csv.reader(chains_file, delimiter=',')
        
        # skip the header
        next(csvreader)

        csv_rows = [row for row in csvreader]

    return sorted(csv_rows, key=lambda x: _sort_by_package_id(x))


def _rows_are_from_same_identifier(row_1: List[str], row_2: List[str]):
    _, _, _, metadataPID_1, _, _ = row_1
    _, _, _, metadataPID_2, _, _ = row_2
    return _are_same_identifier(parse_package_id(metadataPID_1), 
                               parse_package_id(metadataPID_2))
   

def _are_same_identifier(parsed_pid_1: tuple, parsed_pid_2: tuple):
    return parsed_pid_1[:_revision] == parsed_pid_2[:_revision]


def _revisions_are_sequential(parsed_pid_1: tuple, parsed_pid_2: tuple):
    return parsed_pid_1[_revision] < parsed_pid_2[_revision]


def _pair_is_consistent(row_1: List[str], row_2: List[str]):
    pid_1 = parse_package_id(row_1[_metadataPID])
    pid_2 = parse_package_id(row_2[_metadataPID])
    if _are_same_identifier(pid_1, pid_2):
        if _revisions_are_sequential(pid_1, pid_2):
            # Expect an obsolescence relationship
            if row_1[_metadataObsoletedByPID] != row_2[_metadataPID] or \
                row_2[_metadataObsoletesPID] != row_1[_metadataPID]:
                return False
        else:
            print('Unexpected Error - {} and {} are out of sequence'.format(pid_1, pid_2))
    return True


def _display_metadata_pids(i: int, sorted_rows: List[List[str]]):
    row = sorted_rows[i]
    print([row[_metadataPID].replace(_pid_prefix, ''), 
           row[_metadataObsoletesPID].replace(_pid_prefix, ''), 
           row[_metadataObsoletedByPID].replace(_pid_prefix, '')])

    
def _display_context(i: int, sorted_rows: List[List[str]]):
    """ 
    Display pids for a row and nearby rows that are relevant
    """
    if i > 0 and _rows_are_from_same_identifier(sorted_rows[i-1], sorted_rows[i]):
        _display_metadata_pids(i-1, sorted_rows)
    _display_metadata_pids(i, sorted_rows)
    _display_metadata_pids(i+1, sorted_rows)
    if i < len(sorted_rows)-2 and _rows_are_from_same_identifier(sorted_rows[i+1], sorted_rows[i+2]):
        _display_metadata_pids(i+2, sorted_rows)
    print()  # blank line for readability
    
    
def check_consistency(sorted_rows: List[List[str]]):
    for i in range(len(sorted_rows)):
        this_row = sorted_rows[i]
        if i < len(sorted_rows)-1:
            next_row = sorted_rows[i+1]
            if not _pair_is_consistent(this_row, next_row):
                print('Consistency Error')
                _display_context(i, sorted_rows)


def main(obsolescence_chains_csv_file: str):
    sorted_rows = _read_and_sort_obsolescence_chains_csv_file(obsolescence_chains_csv_file)
    check_consistency(sorted_rows)


if __name__ == '__main__':
    check_obsolescence_chains()

