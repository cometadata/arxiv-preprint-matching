# Get Crossref Work Types for Preprint Matches

Adds work type information to preprint matches by looking up DOI types from a reference CSV file.

## Usage
```
python get_crossref_work_types_for_preprint_matches.py -i INPUT.csv -r REFERENCE.csv [-o OUTPUT.csv]
```

## Arguments
- `-i, --input`: Path to input CSV file containing preprint matches with 'matched_doi' column
- `-r, --reference`: Path to reference CSV with DOI type mappings (must have 'doi', 'field_name', and 'value' columns)
- `-o, --output`: Path for output CSV file (default: 'preprint_matches_w_work_types.csv')

## Output
Creates a new CSV file with an additional 'matched_doi_type' column populated from the reference data.