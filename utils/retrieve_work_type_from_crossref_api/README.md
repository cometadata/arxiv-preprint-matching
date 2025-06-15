# Retrieve Work Type from Crossref API

Queries the Crossref API to fill in missing work type values for DOIs in a CSV file.

## Usage
```
python retrieve_work_type_from_crossref_api.py -i INPUT.csv -o OUTPUT.csv -m EMAIL [-u USER_AGENT]
```

## Arguments
- `-i, --input`: Path to input CSV file with 'matched_doi' and 'matched_doi_type' columns
- `-o, --output`: Path for output CSV file with filled work types
- `-m, --mailto`: Email address for Crossref polite pool access
- `-u, --user-agent`: Custom User-Agent string (default: "Retrieve-DOI-Type/1.0")

## Output
Processes rows with empty 'matched_doi_type' values and queries Crossref API to populate them.