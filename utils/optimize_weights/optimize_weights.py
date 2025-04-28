import os
import csv
import json
import sys
import time
import argparse
import itertools
import subprocess
import pandas as pd
from datetime import datetime


PARAM_GRID = {
    'min_score': [0.80, 0.85, 0.90],
    'max_score_diff': [0.03, 0.04, 0.05],
    'weight_year': [0.3, 0.4, 0.5],
    'weight_title': [2.0, 2.2, 2.4],
    'weight_author': [0.8, 1.0, 1.2]
}

MATCHER_SCRIPT = "preprint_match_data_files.py"
EVALUATOR_SCRIPT = "calculate_precision_recall_f-scores.py"

ALL_POSSIBLE_FIELDS = list(PARAM_GRID.keys()) + [
    "Status", "Error", "TP", "FP", "FN", "Precision", "Recall",
    "F0.5", "F1", "F1.5", "Positive References", "Positive Predictions"
]


def run_command(command_list, timeout=300):
    try:
        print(f"Running command: {' '.join(command_list)}")
        process = subprocess.run(
            command_list,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout
        )
        print(f"Command finished with code: {process.returncode}")
        if process.returncode != 0:
            if process.stdout:
                print("Subprocess STDOUT (truncated):")
                print(process.stdout[:500] +
                      ('...' if len(process.stdout) > 500 else ''))
            if process.stderr:
                print("Subprocess STDERR (truncated):")
                print(process.stderr[:500] +
                      ('...' if len(process.stderr) > 500 else ''))
        elif process.stderr:
            print("Subprocess STDERR (potentially warnings):")
            print(process.stderr[:500] +
                  ('...' if len(process.stderr) > 500 else ''))

        return process.stdout, process.stderr, process.returncode
    except subprocess.TimeoutExpired:
        print(f"Error: Command timed out after {timeout} seconds: {' '.join(command_list)}")
        return None, "TimeoutExpired", -1
    except Exception as e:
        print(f"Error running command {' '.join(command_list)}: {e}")
        return None, str(e), -1


def generate_parameter_combinations(grid):
    keys, values = zip(*grid.items())
    combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
    return combinations


def main():
    parser = argparse.ArgumentParser(
        description="Optimize preprint matching strategy weights by iterating through parameter combinations."
    )
    parser.add_argument(
        "-i", "--input-sample", required=True,
        help="Path to the input SAMPLE JSONL file (e.g., 100 records)."
    )
    parser.add_argument(
        "-r", "--reference-csv", required=True,
        help="Path to the reference CSV file (ground truth) for evaluation."
    )
    parser.add_argument(
        "-o", "--output-results-csv", required=True,
        help="Path to save the CSV file containing parameters and evaluation metrics for each run."
    )
    parser.add_argument(
        "-m", "--mailto", required=True,
        help="Email address for Crossref API politeness (passed to matcher script)."
    )
    parser.add_argument(
        "-u", "--user-agent", required=True,
        help="User-Agent string for Crossref API requests (passed to matcher script)."
    )
    parser.add_argument(
        "--matcher-script-path", default=MATCHER_SCRIPT,
        help=f"Path to the preprint_match_data_files.py script (default: {MATCHER_SCRIPT})."
    )
    parser.add_argument(
        "--evaluator-script-path", default=EVALUATOR_SCRIPT,
        help=f"Path to the calculate_precision_recall_f-scores.py script (default: {EVALUATOR_SCRIPT})."
    )
    parser.add_argument(
        "--temp-dir", default="temp_optim_output",
        help="Directory to store temporary output files for each run (default: temp_optim_output)."
    )
    parser.add_argument(
        "--log-level", default="WARNING",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NONE'],
        help="Set the logging level for the matcher script subprocess (default: WARNING)."
    )

    args = parser.parse_args()

    os.makedirs(args.temp_dir, exist_ok=True)
    print(f"Using temporary directory: {args.temp_dir}")

    param_combinations = generate_parameter_combinations(PARAM_GRID)
    total_runs = len(param_combinations)
    print(f"Generated {total_runs} parameter combinations to test.")
    print(f"Results will be saved iteratively to: {args.output_results_csv}")

    output_file_exists = os.path.isfile(args.output_results_csv)
    write_header = not output_file_exists or os.path.getsize(
        args.output_results_csv) == 0

    start_time_total = time.time()

    for i, params in enumerate(param_combinations):
        run_number = i + 1
        print(f"\n--- Starting Run {run_number}/{total_runs} ---")
        print(f"Parameters: {params}")
        start_time_run = time.time()

        result_row = {**params}

        temp_matcher_output_csv = os.path.join(args.temp_dir, f"run_{run_number}_matches.csv")

        matcher_cmd = [
            sys.executable,
            args.matcher_script_path,
            "--input", args.input_sample,
            "--output", temp_matcher_output_csv,
            "--format", "csv",
            "--mailto", args.mailto,
            "--user-agent", args.user_agent,
            "--log-level", args.log_level,
            "--min-score", str(params['min_score']),
            "--max-score-diff", str(params['max_score_diff']),
            "--weight-year", str(params['weight_year']),
            "--weight-title", str(params['weight_title']),
            "--weight-author", str(params['weight_author']),
        ]

        matcher_stdout, matcher_stderr, matcher_retcode = run_command(
            matcher_cmd)

        if matcher_retcode != 0:
            print(f"Error: Matcher script failed for run {run_number}. Skipping evaluation.")
            result_row["Status"] = "Matcher Failed"
            result_row["Error"] = matcher_stderr[:500]

        elif not os.path.exists(temp_matcher_output_csv):
            print(f"Error: Matcher script completed but output file '{temp_matcher_output_csv}' not found for run {run_number}. Skipping evaluation.")
            result_row["Status"] = "Matcher Output Missing"

        else:

            evaluator_cmd = [
                sys.executable,
                args.evaluator_script_path,
                "--reference_csv", args.reference_csv,
                "--test_csv", temp_matcher_output_csv,
                "--json-output"
            ]

            eval_stdout, eval_stderr, eval_retcode = run_command(evaluator_cmd)

            if eval_retcode != 0 or not eval_stdout:
                print(f"Error: Evaluator script failed or produced no output for run {run_number}.")
                result_row["Status"] = "Evaluator Failed"
                result_row["Error"] = eval_stderr[:500]

            else:

                try:
                    metrics = json.loads(eval_stdout)
                    if "error" in metrics:
                        print(f"Error reported by evaluator: {metrics['error']}")
                        result_row["Status"] = "Evaluator JSON Error"
                        result_row["Error"] = metrics['error']
                    else:

                        result_row.update(metrics)
                        result_row["Status"] = "Success"

                except json.JSONDecodeError:
                    print(f"Error: Could not decode JSON output from evaluator for run {run_number}.")
                    print(f"Evaluator STDOUT was: {eval_stdout}")
                    result_row["Status"] = "Evaluator Output Invalid"
                    result_row["Error"] = "JSONDecodeError"

        try:

            with open(args.output_results_csv, 'a', encoding='utf-8') as csvfile:

                writer = csv.DictWriter(
                    csvfile, fieldnames=ALL_POSSIBLE_FIELDS, extrasaction='ignore')

                if write_header:
                    writer.writeheader()
                    write_header = False

                formatted_row = {k: (f"{v:.4f}" if isinstance(v, float) else v) for k, v in result_row.items()}
                writer.writerow(formatted_row)
            print(f"Run {run_number} result appended to {args.output_results_csv}")

        except IOError as e:
            print(f"Error: Could not write results for run {run_number} to CSV '{args.output_results_csv}': {e}", file=sys.stderr)

        run_duration = time.time() - start_time_run
        print(f"Run {run_number} finished in {run_duration:.2f} seconds.")

    total_duration = time.time() - start_time_total
    print(f"\n--- Optimization Complete ---")
    print(f"Total time: {total_duration:.2f} seconds.")
    print(f"All run results saved in: {args.output_results_csv}")

    try:
        results_df = pd.read_csv(args.output_results_csv)
        print(f"\nRead {len(results_df)} results back from CSV for analysis.")
    except FileNotFoundError:
        print(f"Error: Output results file '{args.output_results_csv}' not found for final analysis.", file=sys.stderr)
        return
    except Exception as e:
        print(f"Error reading results file '{args.output_results_csv}' for analysis: {e}", file=sys.stderr)
        return

    successful_runs = results_df[results_df['Status'] == 'Success'].copy()

    if successful_runs.empty:
        print("\nNo successful runs found in the results file to determine the best run.")
        return

    metric_cols_for_sort = ['F1', 'Precision', 'Recall']
    for col in metric_cols_for_sort:
        if col not in successful_runs.columns:
            print(f"Warning: Metric column '{col}' needed for sorting is missing. Cannot determine best run accurately.")

            if col == 'F1':
                return
            metric_cols_for_sort.remove(col)
        else:

            successful_runs[col] = pd.to_numeric(
                successful_runs[col], errors='coerce')

    successful_runs = successful_runs.dropna(subset=['F1'])

    if successful_runs.empty:
        print("\nNo successful runs with valid F1 scores found after conversion.")
        return

    valid_sort_columns = [col for col in [
        'F1', 'Precision', 'Recall'] if col in successful_runs.columns]
    successful_runs = successful_runs.sort_values(
        by=valid_sort_columns,
        ascending=[False] * len(valid_sort_columns)
    )
    best_run = successful_runs.iloc[0]

    print("\n--- Best Run (Max F1 Score) ---")
    print("Parameters:")
    param_cols = list(PARAM_GRID.keys())
    for p in param_cols:

        print(f"  {p}: {best_run.get(p, 'N/A')}")
    print("Metrics:")

    print(f"  Precision: {best_run.get('Precision', 'N/A'):.4f}")
    print(f"  Recall:    {best_run.get('Recall', 'N/A'):.4f}")
    print(f"  F1 Score:  {best_run.get('F1', 'N/A'):.4f}")

    tp = pd.to_numeric(best_run.get('TP'), errors='coerce')
    fp = pd.to_numeric(best_run.get('FP'), errors='coerce')
    fn = pd.to_numeric(best_run.get('FN'), errors='coerce')
    print(f"  (TP={int(tp) if pd.notna(tp) else 'N/A'}, FP={int(fp) if pd.notna(fp) else 'N/A'}, FN={int(fn) if pd.notna(fn) else 'N/A'})")


if __name__ == "__main__":
    main()
