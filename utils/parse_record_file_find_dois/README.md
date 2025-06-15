# Parse Record File Find DOIs

Filters a gzipped JSONL file to extract records matching specific DOIs from a CSV list, splitting output into enumerated subdirectories.

## Usage
```
python parse_record_file_find_dois.py -i INPUT.jsonl.gz -d DOIS.csv [-o OUTPUT_DIR] [-l LINES_PER_FILE] [-u UNMATCHED_LOG]
```

## Arguments
- `-i, --input`: Path to input gzipped JSONL file
- `-d, --doi_csv`: Path to CSV file with 'doi' column containing target DOIs
- `-o, --output_base_dir`: Base output directory for enumerated subdirectories (default: 'output_data')
- `-l, --lines_per_file`: Maximum lines per output file (default: 10000)
- `-u, --unmatched_log`: CSV file for logging unmatched DOIs (default: 'unmatched_dois.csv')

## Output
Creates enumerated subdirectories (1, 2, 3, ...) each containing a 'data.jsonl.gz' file with matching records.