#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import requests

import click


@click.command()
@click.argument('obsolescence_info_sorted_csv_file')
def check_consistency_of_obsolescence_info(obsolescence_info_sorted_csv_file: str):
    """
    Read CSV file with pid, obsoletes, obsoletedBy obtained by running get_system_metadata_obsolescence_info.py.
I.e., this file contains the currently existing obsolescence info on a MN or CN of interest. File is assumed to
have been sorted.
For each package, check the versions and obsolescence chains for internal consistency and for
consistency with the versions lists in PASTA.

Arguments: \n
        OBSOLESCENCE_INFO_SORTED_CSV_FILE: sorted CSV file with PID, obsoletes, obsoletedBy
    """
    main(obsolescence_info_sorted_csv_file)


def parsePID(pid: str):
    PASTA_PREFIX = 'https://pasta.lternet.edu/package/metadata/eml/'
    identifier, scope, version = pid.strip().replace(PASTA_PREFIX, '').split('/')
    return ('{}/{}'.format(identifier, scope), version)


def getVersion(pid: str):
    if not pid:
        return None
    _, version = parsePID(pid)
    return version


def main(input_filename: str):

    # For simplicity, assume the input is sorted by pid
    # Read the input file:  pid, obsoletes, obsoletedBy

    # packages dict:  
    #  key of the form 'knb-lter-and/2719'
    #  value is dict, with keys
    #    'versions' - list of versions
    #    'obsoletes' - list of obsoletes 
    #    'obsoletedBy' - list of obsoletedBy
    # Consistency checks:
    #    List of obsoletes == versions[1:]
    #    List of obsoletedBy == versions[:-1]

    packages = collections.OrderedDict()
    # Read in the input CSV file
    rows = []
    with open(input_filename, 'rt') as input_file:
        rows = input_file.readlines()
        # Skip header
        rows = rows[1:]
        for row in rows:
            row = row.strip()
            pid, obsoletes, obsoletedBy = row.split(',')
            package, version = parsePID(pid)
            if package not in packages:
                packages[package] = {'versions': [], 'obsoletes': [], 'obsoletedBy': []}
            package = packages[package]
            package['versions'].append(version)
            obsoletes = getVersion(obsoletes)
            if obsoletes:
                package['obsoletes'].append(obsoletes)
            obsoletedBy = getVersion(obsoletedBy)
            if obsoletedBy:
                package['obsoletedBy'].append(obsoletedBy)

    # Check for internal consistency
    print('Checking for internal consistency...')
    for package_key in packages:
        package = packages[package_key]
        package['versions'].sort(key=int)
        package['obsoletes'].sort(key=int)
        package['obsoletedBy'].sort(key=int)
        obsoletes_error = (package['obsoletes'] != package['versions'][:-1])
        obsoletedBy_error = (package['obsoletedBy'] != package['versions'][1:])
        if obsoletes_error or obsoletedBy_error:
            print('{}  {}'.format(package_key, ' '.join(package['versions'])))
            if obsoletes_error:
                print('   ERROR: obsoletes = {} != {}'.format(' '.join(package['obsoletes']), 
                                                              ' '.join(package['versions'][:-1])))
            if obsoletedBy_error:
                print('   ERROR: obsoletedBy = {} != {}'.format(' '.join(package['obsoletedBy']), 
                                                                ' '.join(package['versions'][1:])))
    # Check against PASTA
    print()
    print('Checking against PASTA... {} packages to check'.format(len(packages)))
    count = 0
    for package_key in packages:
        count += 1
        package = packages[package_key]
        url = "https://pasta.lternet.edu/package/eml/" + package_key
        pasta_versions = requests.get(url).text.replace('\n', ' ')
        versions = ' '.join(package['versions'])
        if pasta_versions != versions:
            print('{} - PASTA: {} - Found: {}'.format(package_key, pasta_versions, versions))
        if count % 100 == 0:
            print('count={}'.format(count))


if __name__ == '__main__':
    check_consistency_of_obsolescence_info()
    # input_filename = '0-53040_gmn_obsolescence_info_sorted.csv'
    # main(input_filename)
