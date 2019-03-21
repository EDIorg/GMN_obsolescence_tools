# GMN_obsolescence_tools
Tools for fixing up the obsolescence chains in system metadata for packages in a DataONE Generic Member Node.

The workflow consists of several steps: a server-side SQL query followed by a sequence of four Python scripts. The Python scripts can be run separately or via the master script repair_obsolescence_batch.py.


## Sample Workflow Using the Master Script
Suppose the member node to be updated is gmn.lternet.edu. The workflow is as follows:

#### 1. Server-side PostgreSQL query to get a list of DOIs associated with the GMN
- E.g., using the "gmn" account on the GMN server:
> `psql -d gmn3 -P pager -c "select did from app_idnamespace where did like 'doi:10.6073/pasta/%'" > doi_list.csv`

#### 2. repair_obsolescence_batch.py
Run each of the other python scripts (see below). It can be run for a subset of the DOIs. The intention is that the DOIs can be processed in batches, so as not to overwhelm the DataONE Coordinating Node.
- E.g.,
> ./repair_obsolescence_batch doi_list.csv 3000 4000 gmn.lternet.edu ../certs/urn_node_LTER-2.pem output_3000_4000
This would run the repair sequence on DOIs 3000-3999 in the list, using the designated X.509 certificate, and saving output in subdirectory output_3000_4000. To run on the entire set of DOIs, use 0 and -1 as start and end indices. The output consists of the output of each script in the sequence, including their stdout output, in separate files.


## Sample Workflow Running the Scripts Manually
For testing and troubleshooting, it may be desirable to run the scripts one step at a time.
Suppose the member node to be updated is gmn.lternet.edu. The workflow is as follows:

#### 1. Server-side PostgreSQL query to get a list of DOIs associated with the GMN
Same as in the workflow above. To run a small set of DOIs, edit the list to use as input to subsequent steps.

#### 2. get_obsolescence_chains.py
Generate a CSV file containing the obsolescence chains for DOIs associated with a GMN.
The output CSV file has a header row. Columns are: doi, obsoletes, obsoletedBy, metadataPID, metadataPIDObsoletes, metadataPIDObsoletedBy
- E.g., 
> `./get_obsolescence_chains.py doi_list.csv lternet.edu_obsolescence_chains.csv -m gmn.lternet.edu`

This may take 3-4 hours to run for the full set of DOIs.

#### 3. resolve_unresolved_dois.py 
Update the output CSV from the previous step, replacing UNRESOLVED entries by resolving the DOIs and parsing their landing pages to get the corresponding Package IDs.
- E.g., 
> ./resolve_unresolved_dois.py lternet.edu_obsolescence_chains.csv lternet.edu_obsolescence_chains_resolved.csv -m gmn.lternet.edu

#### 4. update_obsolescence_chains.py
Construct updated system metadata for packages needing obsolescence chains, and use the REST API to update the system metadata for packages whose metadata needs updating. Optionally, create a TSV file with the updated metadata.
- E.g., 
> ./update_obsolescence_chains.py lternet.edu_obsolescence_chains_resolved.csv "path to X.509 client certificate" -m gmn.lternet.edu -o lternet.edu_updates.tsv

This may take 1-2 hours to run for the full set of DOIs.

#### 5. check_metadata_obsolescence_entries.py
Check the obsolescence chains in system metadata against the expected values based on the obsolescence chains in ORE objects. The latter are read from the CSV file generated in step 3, above.
- E.g., 
> ./check_metadata_obsolescence_entries.py lternet.edu_obsolescence_chains_resolved.csv -m gmn.lternet.edu

By default, we check obsolescence chains only for objects that have obsolescence information in the OREs. To check all objects, use the --deep option. Running the default case may take 30-60 minutes for the full set of DOIs.
