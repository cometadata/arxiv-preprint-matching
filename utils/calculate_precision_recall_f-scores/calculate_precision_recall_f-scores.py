import csv
import sys
import json
import argparse
from collections import defaultdict


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Evaluate DOI matching results using Precision, Recall, and F-beta scores."
    )
    parser.add_argument(
        "-r", "--reference_csv",
        required=True,
        help="Path to the reference CSV file (ground truth). Expected columns: 'doi', 'related_doi'."
    )
    parser.add_argument(
        "-t", "--test_csv",
        required=True,
        help="Path to the test results CSV file. Expected columns: 'input_doi', 'matched_doi'."
    )
    parser.add_argument(
        "-c", "--output_csv",
        help="Optional path to save the evaluation metrics to a CSV file."
    )
    parser.add_argument(
        "--json-output",
        action='store_true',
        help="If set, print evaluation metrics as a JSON object to stdout and suppress other console output."
    )
    return parser.parse_args()


def load_csv_to_dict(file_path, key_col, value_col, suppress_errors=False):
    data_dict = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if key_col not in reader.fieldnames or value_col not in reader.fieldnames:
                if not suppress_errors:
                    print(f"Error: Missing required columns ('{key_col}', '{value_col}') in {file_path}", file=sys.stderr)
                return None
            for row in reader:
                key = row[key_col].strip().lower() if row[key_col] else None
                value = row[value_col].strip(
                ).lower() if row[value_col] else None
                if key:
                    data_dict[key] = value
    except FileNotFoundError:
        if not suppress_errors:
            print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        if not suppress_errors:
            print(f"Error reading CSV file {file_path}: {e}", file=sys.stderr)
        return None
    return data_dict


def calculate_f_beta(precision, recall, beta):
    if precision == 0.0 and recall == 0.0:
        return 0.0
    if beta <= 0:
        raise ValueError("Beta must be a positive number.")

    beta_sq = beta ** 2
    numerator = (1 + beta_sq) * (precision * recall)
    denominator = (beta_sq * precision) + recall

    if denominator == 0:
        return 0.0
    return numerator / denominator


def calculate_metrics(reference_map, test_map):
    tp = 0
    fp = 0
    fn = 0

    positive_references = {k: v for k, v in reference_map.items() if v}
    num_positive_references = len(positive_references)

    positive_predictions = 0

    for input_doi, matched_doi in test_map.items():
        input_doi_norm = input_doi.strip().lower() if input_doi else None
        matched_doi_norm = matched_doi.strip().lower() if matched_doi else None

        is_positive_prediction = bool(matched_doi_norm)
        should_be_positive = input_doi_norm in positive_references

        if is_positive_prediction:
            positive_predictions += 1
            if should_be_positive:
                correct_match_doi = positive_references[input_doi_norm]
                if matched_doi_norm == correct_match_doi:
                    tp += 1
                else:
                    fp += 1
            else:
                fp += 1
        else:
            if should_be_positive:
                fn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    # Recall denominator is total *actual* positives
    recall = tp / num_positive_references if num_positive_references > 0 else 0.0

    f0_5 = calculate_f_beta(precision, recall, 0.5)
    f1 = calculate_f_beta(precision, recall, 1.0)
    f1_5 = calculate_f_beta(precision, recall, 1.5)

    return {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "Precision": precision,
        "Recall": recall,
        "F0.5": f0_5,
        "F1": f1,
        "F1.5": f1_5,
        "Positive References": num_positive_references,
        "Positive Predictions": positive_predictions
    }


def write_metrics_to_csv(metrics, output_file, suppress_print=False):
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            fieldnames = [
                "Positive References", "Positive Predictions",
                "TP", "FP", "FN",
                "Precision", "Recall",
                "F0.5", "F1", "F1.5"
            ]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            formatted_metrics = {k: (f"{v:.4f}" if isinstance(v, float) else v)
                                 for k, v in metrics.items() if k in fieldnames}
            writer.writerow(formatted_metrics)
        if not suppress_print:
            print(f"Evaluation metrics saved to: {output_file}")
    except IOError as e:
        if not suppress_print:
            print(f"Error writing metrics to CSV file {output_file}: {e}", file=sys.stderr)
    except Exception as e:
        if not suppress_print:
            print(f"An unexpected error occurred while writing metrics CSV: {e}", file=sys.stderr)


def main():
    args = parse_arguments()
    suppress_console = args.json_output

    if not suppress_console:
        print(f"Loading reference data from: {args.reference_csv}")
    reference_data = load_csv_to_dict(
        args.reference_csv, 'doi', 'related_doi', suppress_errors=suppress_console)
    if reference_data is None:
        if not suppress_console:
            sys.exit(1)
        else:
            print(json.dumps({"error": f"Failed to load reference CSV: {args.reference_csv}"}), file=stdout)
            sys.exit(1)

    if not suppress_console:
        print(f"Loading test results from: {args.test_csv}")
    test_data = load_csv_to_dict(
        args.test_csv, 'input_doi', 'matched_doi', suppress_errors=suppress_console)
    if test_data is None:
        if not suppress_console:
            sys.exit(1)
        else:
            print(json.dumps({"error": f"Failed to load test CSV: {args.test_csv}"}), file=stdout)
            sys.exit(1)

    if not suppress_console:
        print("\nCalculating metrics...")
    metrics = calculate_metrics(reference_data, test_data)

    if args.json_output:
        print(json.dumps(metrics, indent=None))
    else:
        print("\n--- Evaluation Results ---")
        print(f"Positive Relations in Reference: {metrics['Positive References']}")
        print(f"Positive Predictions in Test:    {metrics['Positive Predictions']}")
        print(f"True Positives (TP):  {metrics['TP']}")
        print(f"False Positives (FP): {metrics['FP']}")
        print(f"False Negatives (FN): {metrics['FN']}")
        print("--------------------------")
        print(f"Precision: {metrics['Precision']:.4f}")
        print(f"Recall:    {metrics['Recall']:.4f}")
        print("--------------------------")
        print(f"F0.5 Score (Prec > Rec): {metrics['F0.5']:.4f}")
        print(f"F1 Score   (Balanced):   {metrics['F1']:.4f}")
        print(f"F1.5 Score (Rec > Prec): {metrics['F1.5']:.4f}")
        print("--------------------------")

    if args.output_csv:
        write_metrics_to_csv(metrics, args.output_csv,
                             suppress_print=args.json_output)


if __name__ == "__main__":
    main()
