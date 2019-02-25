# GMN_obsolescence_tools
Tools for fixing up the obsolescence chains in system metadata for packages in a DataONE Generic Member Node

## Sample Workflow
Suppose the member node to be updated is gmn.lternet.edu. The workflow is as follows:

#### 1. Server-side PostgreSQL query to get a list of DOIs associated with the GMN
- E.g., using the "gmn" account on the GMN server:
> `psql -d gmn3 -P pager -c "select did from app_idnamespace where did like 'doi:10.6073/pasta/%'" > doi_list.csv`

#### 2. get_obsolescence_chains.py
Generate a CSV file containing the obsolescence chains for DOIs associated with a GMN.
The output CSV file has a header row. Columns are: doi, obsoletes, obsoletedBy, metadataPID, metadataPIDObsoletes, metadataPIDObsoletedBy
- E.g., 
> `./get_obsolescence_chains.py doi_list.csv lternet.edu_obsolescence_chains.csv -m gmn.lternet.edu`

This may take 3-4 hours to run.

#### 3. resolve_unresolved_dois.py 
Update the output CSV from the previous step, replacing UNRESOLVED entries by resolving the DOIs and parsing their landing pages to get the corresponding Package IDs.
- E.g., 
> ./resolve_unresolved_dois.py lternet.edu_obsolescence_chains.csv lternet.edu_obsolescence_chains_resolved.csv -m gmn.lternet.edu

#### 4. update_obsolescence_chains.py
Construct updated system metadata for packages needing obsolescence chains, and use the REST API to update the system metadata for packages whose metadata needs updating. Optionally, create a TSV file with the updated metadata.
- E.g., 
> ./update_obsolescence_chains.py lternet.edu_obsolescence_chains_resolved.csv "path to X.509 client certificate" -m gmn.lternet.edu -o lternet.edu_updates.tsv

This may take 1-2 hours to run.

#### 5. check_metadata_obsolescence_entries.py
Check the obsolescence chains in system metadata against the expected values based on the obsolescence chains in ORE objects. The latter are read from the CSV file generated in step 3, above.
- E.g., 
> ./check_metadata_obsolescence_entries.py lternet.edu_obsolescence_chains_resolved.csv -m gmn.lternet.edu

By default, it checks obsolescence chains only for objects that have obsolescence information in the OREs. To check all objects, use the --deep option. Running the default case may take 30-60 minutes.
