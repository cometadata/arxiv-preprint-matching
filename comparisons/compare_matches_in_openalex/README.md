# Compare Matches

Compares arXiv preprint matches with OpenAlex work locations to identify overlapping preprint matches.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python compare_matches.py -i input.csv -o output.csv [-m email] [-l LOG_LEVEL]
```

**Required:**
- `-i, --input`: Input CSV file with `input_doi` and `matched_doi` columns
- `-o, --output`: Output CSV file path

**Optional:**
- `-m, --mailto`: Email for OpenAlex polite pool access (faster requests)
- `-l, --log-level`: DEBUG, INFO, WARNING, ERROR (default: INFO)

## Process

1. Extracts arXiv ID from input DOI
2. Queries OpenAlex API for matched DOI work data
3. Checks if arXiv ID appears in work's location URLs
4. Outputs original data plus `matched_in_openalex` column (TRUE/FALSE/ERROR)

Progress displayed every 50 rows. Rate limiting and retry logic included.