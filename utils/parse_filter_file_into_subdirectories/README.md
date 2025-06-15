# Parse Filter File into Subdirectories

Filters a gzipped JSONL file to extract records with empty 'relatedIdentifiers' lists, splitting output into enumerated subdirectories.

## Usage
```
python parse_filter_file_into_subdirectories.py -i INPUT.jsonl.gz [-o OUTPUT_DIR] [-l LINES_PER_FILE]
```

## Arguments
- `-i, --input`: Path to input gzipped JSONL file
- `-o, --output_base_dir`: Base output directory for enumerated subdirectories (default: 'output_data')
- `-l, --lines_per_file`: Maximum lines per output file (default: 10000)

## Output
Creates enumerated subdirectories (1, 2, 3, ...) each containing a 'data.jsonl.gz' file with records that have empty 'relatedIdentifiers' arrays.