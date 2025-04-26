import argparse
import csv
import sys
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
    return parser.parse_args()

def load_csv_to_dict(file_path, key_col, value_col):
    data_dict = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if key_col not in reader.fieldnames or value_col not in reader.fieldnames:
                print(f"Error: Missing required columns ('{key_col}', '{value_col}') in {file_path}", file=sys.stderr)
                return None
            for row in reader:
                key = row[key_col].strip().lower() if row[key_col] else None
                value = row[value_col].strip().lower() if row[value_col] else None
                if key:
                    data_dict[key] = value
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error reading CSV file {file_path}: {e}", file=sys.stderr)
        return None
    return data_dict

def calculate_f_beta(precision, recall, beta):
    if precision + recall == 0:
        return 0.0
    beta_sq = beta ** 2
    numerator = (1 + beta_sq) * (precision * recall)
    denominator = (beta_sq * precision) + recall
    if denominator == 0:
        return 0.0
    return numerator / denominator

def calculate_metrics(reference_map, test_map):
    tp = 0
    fp = 0
    positive_references = {k: v for k, v in reference_map.items() if v}

    for input_doi, matched_doi in test_map.items():
        if matched_doi:
            if input_doi in positive_references:
                if matched_doi == positive_references[input_doi]:
                    tp += 1
                else:
                    fp += 1
            else:
                fp += 1

    fn = len(positive_references) - tp
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
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
        "Positive References": len(positive_references),
        "Positive Predictions": tp + fp
    }

def write_metrics_to_csv(metrics, output_file):
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
            formatted_metrics = {k: (f"{v:.4f}" if isinstance(v, float) else v) for k, v in metrics.items()}
            writer.writerow(formatted_metrics)
        print(f"Evaluation metrics saved to: {output_file}")
    except IOError as e:
        print(f"Error writing metrics to CSV file {output_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred while writing metrics CSV: {e}", file=sys.stderr)


def main():
    args = parse_arguments()

    print(f"Loading reference data from: {args.reference_csv}")
    reference_data = load_csv_to_dict(args.reference_csv, 'doi', 'related_doi')
    if reference_data is None:
        sys.exit(1)

    print(f"Loading test results from: {args.test_csv}")
    test_data = load_csv_to_dict(args.test_csv, 'input_doi', 'matched_doi')
    if test_data is None:
        sys.exit(1)

    print("\nCalculating metrics...")
    metrics = calculate_metrics(reference_data, test_data)

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
    print(f"F0.5 Score: {metrics['F0.5']:.4f}")
    print(f"F1 Score:   {metrics['F1']:.4f}")
    print(f"F1.5 Score: {metrics['F1.5']:.4f}")
    print("--------------------------")

    if args.output_csv:
        write_metrics_to_csv(metrics, args.output_csv)

if __name__ == "__main__":
    main()