# Parameter Optimization for Preprint Matching

A grid search utility to find optimal parameters for preprint-to-publication matching by evaluating different combinations of matching thresholds and weights.

## Installation
```
pip install pandas
```

## Parameter Grid
The script tests combinations of the following parameters (edit `PARAM_GRID` in the code to customize):
```python
PARAM_GRID = {
    'min_score': [0.80, 0.85, 0.90],
    'max_score_diff': [0.03, 0.04, 0.05],
    'weight_year': [0.3, 0.4, 0.5],
    'weight_title': [2.0, 2.2, 2.4],
    'weight_author': [0.8, 1.0, 1.2]
}
```

## Usage
```
python optimize_preprint_matcher.py \
  -i sample_input.jsonl \
  -r ground_truth.csv \
  -o results.csv \
  -m your.email@example.com \
  -u "Your Project Name/1.0"
```

## Arguments
Required:
- `-i, --input-sample`: Input JSONL file with preprint data
- `-r, --reference-csv`: Ground truth CSV for evaluation  
- `-o, --output-results-csv`: Output file for results
- `-m, --mailto`: Email address for Crossref API
- `-u, --user-agent`: User-Agent for API requests

Optional:
- `--matcher-script-path`: Path to matcher script (default: `preprint_match_data_files.py`)
- `--evaluator-script-path`: Path to evaluation script (default: `calculate_precision_recall_f-scores.py`)
- `--temp-dir`: Directory for temporary files (default: `temp_optim_output`)
- `--log-level`: Logging verbosity (default: `WARNING`)

The script automatically identifies and reports the parameter combination with the highest F1 score.

## Required External Scripts
The optimizer works with two companion scripts:
1. `preprint_match_data_files.py` - performs the preprint-to-publication matching
2. `calculate_precision_recall_f-scores.py` - calculates precision/recall metrics

Ensure both scripts are in the same directory or specify their locations with the appropriate arguments.