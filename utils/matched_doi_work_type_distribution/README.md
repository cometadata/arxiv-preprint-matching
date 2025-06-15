# Matched DOI Work Type Distribution

Calculates and saves the distribution of work types in matched DOI data.

## Usage
```
python matched_doi_work_type_distribution.py -i INPUT.csv -o OUTPUT.csv
```

## Arguments
- `-i, --input`: Path to input CSV file containing 'matched_doi_type' column
- `-o, --output`: Path for output CSV file with distribution results

## Output
Creates a CSV file with count and percentage distribution of each work type value, including null values.