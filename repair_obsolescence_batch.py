#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys

import click


@click.command()
@click.argument('doi_file')
@click.argument('start')
@click.argument('end')
@click.argument('member_node')
@click.argument('path_to_x509_cert')
@click.argument('output_file_prefix')
@click.option(
    "-t",
    default=None,
    help="TSV file with DOI to PID mapping"
)
def repair_obsolescence_batch(doi_file: str, start: str, end: str, member_node: str, 
                              path_to_x509_cert: str, output_file_prefix: str, t: str):
    """
    Run a batch of DOIs through the obsolescence chain repair process.

Arguments: \n
        DOI_FILE: text file containing a list of DOIs, one per line \n
        START: 0-based index of first DOI to process in this batch\n
        END: 0-based index of last DOI to process; -1 for end of list\n
        MEMBER_NODE: e.g., gmn.lternet.edu
        PATH_TO_X509_CERT: fully-qualified path/filename for x509 certificate\n
        OUTPUT_FILE_PREFIX: prefix for names of files generated in the process

        For description of output files, see:
          - get_obsolescence_chains.py
          - resolve_unresolved_dois.py
          - update_obsolescence_chains.py
          - check_metadata_obsolescence_entries.py
    """

    # Check the Python version
    if (sys.version_info < (3, 7)):
        print('Requires Python 3.7 or later')
        exit(0)

    main(doi_file, int(start), int(end), member_node, path_to_x509_cert, output_file_prefix, t)


def read_doi_excerpt(doi_filename: str, start: int, end: int, output_file_prefix: str):
    dois = []
    with open(doi_filename, mode='r') as doi_file:
        for doi in doi_file:
            doi = doi.strip()
            if not doi:
                continue
            dois.append(doi)
    excerpt = dois[start:end]
    excerpt_filename = output_file_prefix + '_dois.csv'
    with open(excerpt_filename, mode='w') as excerpt_file:
        for doi in excerpt:
            excerpt_file.write('{}\n'.format(doi))
    return excerpt_filename


def get_obsolescence_chains(excerpt_filename: str, output_file_prefix: str, mn: str):
    chains_filename = output_file_prefix + '_obsolescence_chains.csv'
    stdout_filename = output_file_prefix + '_obsolescence_chains.stdout'
    cmdline = './get_obsolescence_chains.py {} {} -m {} > {}'.format(excerpt_filename, 
        chains_filename, mn, stdout_filename)
    print(cmdline)
    os.system(cmdline)
    return chains_filename


def resolve_unresolved_dois(chains_filename: str, output_file_prefix: str, tsv_file_name: str):
    resolved_filename = output_file_prefix + '_obsolescence_chains_resolved.csv'
    stdout_filename = output_file_prefix + '_obsolescence_chains_resolved.stdout'
    if tsv_file_name:
        cmdline = './resolve_unresolved_dois.py {} {} -t {} > {}'.format(chains_filename, 
            resolved_filename, tsv_file_name, stdout_filename)
    else:
        cmdline = './resolve_unresolved_dois.py {} {} > {}'.format(chains_filename, 
            resolved_filename, stdout_filename)        
    print(cmdline)
    os.system(cmdline)
    return resolved_filename


def update_obsolescence_chains(resolved_filename: str, path_to_x509_cert: str, 
                               output_file_prefix: str, mn: str):
    updates_filename = output_file_prefix + '_updates.tsv'
    stdout_filename = output_file_prefix + '_updates.stdout'
    cmdline = './update_obsolescence_chains.py {} {} -m {} -o {} > {}'.format(resolved_filename, 
        path_to_x509_cert, mn, updates_filename, stdout_filename)
    print(cmdline)
    os.system(cmdline)


def check_metadata_obsolescence_entries(resolved_filename: str, output_file_prefix: str, mn: str):
    results_filename = output_file_prefix + '_results.txt'
    cmdline = './check_metadata_obsolescence_entries.py {} -m {} > {}'.format(resolved_filename, 
        mn, results_filename)
    print(cmdline)
    os.system(cmdline)


def main(doi_filename: str, start: int, end: int, mn: str, path_to_x509_cert: str, 
         output_file_prefix: str, tsv_file_name: str):
    excerpt_filename = read_doi_excerpt(doi_filename, start, end, output_file_prefix)
    chains_filename = get_obsolescence_chains(excerpt_filename, output_file_prefix, mn)
    resolved_filename = resolve_unresolved_dois(chains_filename, output_file_prefix, tsv_file_name)
    update_obsolescence_chains(resolved_filename, path_to_x509_cert, output_file_prefix, mn)
    check_metadata_obsolescence_entries(resolved_filename, output_file_prefix, mn)


if __name__ == '__main__':
    print(datetime.datetime.now().strftime('%H:%M:%S'))
    try:
        repair_obsolescence_batch()
    finally:
        # click exits via sys.exit(), so we use try/finally to get the
        # ending datetime to display
        print(datetime.datetime.now().strftime('%H:%M:%S'))
