# Find Duplicate DOI in Preprint Matches

Find and reports all unique DOI and rows in the preprint matching CSV file output that share a duplicate `matched_doi` value.

## Usage

```bash
python find_duplicates.py -i <input.csv> [-o <duplicates.csv>] [-r <report.txt>]
```

### Arguments

* `-i`, `--input`: (Required) Path to the input CSV file.
* `-o`, `--output`: (Optional) Path for the output CSV containing duplicate rows. Default: `duplicates.csv`.
* `-r`, `--report`: (Optional) Path for the summary report. Default: `report.txt`.