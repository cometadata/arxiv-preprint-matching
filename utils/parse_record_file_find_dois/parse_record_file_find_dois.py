import os
import csv
import sys
import json
import gzip
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Parse a gzipped JSONL input file, filter records based on a list of DOIs from a CSV file, "
            "and write matching records to new gzipped files in enumerated subdirectories. "
            "Logs any DOIs from the CSV that were not found in the input file."
        )
    )
    parser.add_argument(
        "-i", "--input", required=True, help="Path to the input JSONL.gz file."
    )
    parser.add_argument(
        "-d", "--doi_csv", required=True, help="Path to the input CSV file containing a 'doi' column."
    )
    parser.add_argument(
        "-o", "--output_base_dir", default='output_data', help=("Path to the base output directory where enumerated subdirectories (1, 2, 3, ...) "
                                                                "will be created. Default: output_data")
    )
    parser.add_argument(
        "-l", "--lines_per_file", type=int, default=10000, help=("Maximum number of lines per output gzipped file. Each such file "
                                                                 "will be placed in a new enumerated subdirectory. Default: 10000")
    )
    parser.add_argument(
        "-u", "--unmatched_log", default='unmatched_dois.csv', help="Filename for the CSV log of DOIs that were not found. Default: unmatched_dois.csv"
    )
    return parser.parse_args()


def read_dois_from_csv(file_path):
    dois = set()
    try:
        with open(file_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            if 'doi' not in reader.fieldnames:
                sys.stderr.write(f"Error: CSV file '{file_path}' must have a 'doi' column header.\n")
                return None
            for row in reader:
                doi = row.get('doi')
                if doi:
                    dois.add(doi.strip())
        print(f"Successfully loaded {len(dois)} unique DOIs from '{file_path}'.")
        return dois
    except FileNotFoundError:
        sys.stderr.write(f"Error: DOI CSV file not found at '{file_path}'.\n")
        return None
    except Exception as e:
        sys.stderr.write(f"Error reading DOI CSV file '{file_path}': {e}\n")
        return None


def check_condition_by_doi(line_str, doi_set):
    try:
        data = json.loads(line_str)
        record_doi = data.get("id")
        if record_doi and record_doi in doi_set:
            return line_str.strip(), record_doi
    except json.JSONDecodeError:
        pass
    except Exception as e:
        sys.stderr.write(f"Warning: An unexpected error occurred while processing a line: {e}\n")
    return None, None


def create_new_output_file_and_subdir(base_output_dir, subdir_enumeration):
    subdir_path = os.path.join(base_output_dir, str(subdir_enumeration))
    try:
        os.makedirs(subdir_path, exist_ok=True)
    except OSError as e:
        sys.stderr.write(f"Error: Could not create subdirectory {subdir_path}: {e}\n")
        return None, None

    output_file_name = "data.jsonl.gz"
    output_file_path = os.path.join(subdir_path, output_file_name)

    try:
        file_handler = gzip.open(output_file_path, 'wt', encoding='utf-8')
        print(f"Creating new output file: {output_file_path}")
        return file_handler, output_file_path
    except IOError as e:
        sys.stderr.write(f"Error: Could not open output file {output_file_path} for writing: {e}\n")
        return None, None


def write_unmatched_dois(unmatched_doi_set, file_path):
    if not unmatched_doi_set:
        print("\nAll DOIs from the input CSV were found and matched.")
        return

    print(f"\nWriting {len(unmatched_doi_set)} unmatched DOIs to '{file_path}'...")
    try:
        with open(file_path, 'w', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['doi'])
            for doi in sorted(list(unmatched_doi_set)):
                writer.writerow([doi])
        print("Successfully wrote unmatched DOIs log.")
    except IOError as e:
        sys.stderr.write(f"Error: Could not write unmatched DOIs to '{file_path}': {e}\n")


def main():
    args = parse_arguments()

    input_file_path = args.input
    doi_csv_path = args.doi_csv
    base_output_dir = args.output_base_dir
    lines_per_output_file_limit = args.lines_per_file
    unmatched_log_path = args.unmatched_log

    target_dois = read_dois_from_csv(doi_csv_path)
    if target_dois is None:
        sys.exit(1)

    unmatched_dois = target_dois.copy()

    if not os.path.isfile(input_file_path):
        sys.stderr.write(f"Error: Input file '{input_file_path}' not found or is not a file.\n")
        sys.exit(1)

    try:
        os.makedirs(base_output_dir, exist_ok=True)
    except OSError as e:
        sys.stderr.write(f"Error: Could not create base output directory '{base_output_dir}': {e}\n")
        sys.exit(1)

    total_matching_records_count = 0
    lines_in_current_file_count = 0
    current_output_subdir_number = 0
    output_file_handler = None
    current_output_file_path = None

    try:
        with gzip.open(input_file_path, 'rt', encoding='utf-8') as infile:
            for line_content in infile:
                processed_line, matched_doi = check_condition_by_doi(
                    line_content, target_dois)

                if processed_line:
                    total_matching_records_count += 1

                    unmatched_dois.discard(matched_doi)

                    if output_file_handler is None or lines_in_current_file_count >= lines_per_output_file_limit:
                        if output_file_handler:
                            output_file_handler.close()
                            print(f"Closed output file: {current_output_file_path}")

                        current_output_subdir_number += 1
                        output_file_handler, current_output_file_path = create_new_output_file_and_subdir(
                            base_output_dir, current_output_subdir_number
                        )

                        if output_file_handler is None:
                            sys.stderr.write(
                                "Critical error: Failed to create output file. Aborting.\n")
                            sys.exit(1)
                        lines_in_current_file_count = 0

                    output_file_handler.write(processed_line + '\n')
                    lines_in_current_file_count += 1

    except FileNotFoundError:
        sys.stderr.write(f"Error: Input file '{input_file_path}' disappeared during processing.\n")
        sys.exit(1)
    except IOError as e:
        sys.stderr.write(f"Error reading input file '{input_file_path}': {e}\n")
        sys.exit(1)
    except Exception as e:
        sys.stderr.write(f"An unexpected error occurred during processing: {e}\n")
        sys.exit(1)
    finally:
        if output_file_handler and not output_file_handler.closed:
            output_file_handler.close()
            if current_output_file_path:
                print(f"Closed final output file: {current_output_file_path}")

    write_unmatched_dois(unmatched_dois, unmatched_log_path)

    print(f"\nProcessing complete.")
    print(f"Total number of records matched from the DOI list: {total_matching_records_count}")


if __name__ == "__main__":
    main()
