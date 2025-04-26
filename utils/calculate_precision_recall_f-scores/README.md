# calculate_precision_recall_f-scores.py

Script for evaluating preprint matching results. Calculates precision, recall, and F-scores

## Usage
```
python calculate_precision_recall_f-scores.py -r REFERENCE_CSV -t TEST_CSV [-c OUTPUT_CSV]
```

## Inputs
- Reference CSV: Ground truth with 'doi' and 'related_doi' columns
- Test CSV: Matching results with 'input_doi' and 'matched_doi' columns

## Output
The script logs an evaluation summary to console and can optionally save metrics to a CSV file.