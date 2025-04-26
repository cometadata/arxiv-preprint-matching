import os
import sys
import json
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Merge JSON files from a directory into a single JSONL file."
    )
    parser.add_argument(
        "-i", "--input_dir",
        required=True,
        help="Path to the directory containing input JSON files."
    )
    parser.add_argument(
        "-o", "--output_file",
        required=True,
        help="Path to the output JSONL file."
    )
    return parser.parse_args()


def process_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error processing file {file_path}: {e}", file=sys.stderr)
        return None


def merge_json_files(input_dir, output_file):
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory not found or is not a directory: {input_dir}", file=sys.stderr)
        return

    processed_files_count = 0
    error_files_count = 0

    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            for root, _, files in os.walk(input_dir):
                for filename in files:
                    if filename.lower().endswith(".json"):
                        file_path = os.path.join(root, filename)
                        print(f"Processing: {file_path}")
                        json_data = process_json_file(file_path)

                        if json_data is not None:
                            try:
                                json_line = json.dumps(
                                    json_data, separators=(',', ':'))
                                outfile.write(json_line + '\n')
                                processed_files_count += 1
                            except Exception as e:
                                print(f"Error writing data from {file_path} to output: {e}", file=sys.stderr)
                                error_files_count += 1
                        else:
                            error_files_count += 1

        print(f"\nProcessing complete.")
        print(f"Successfully processed and merged {processed_files_count} JSON files.")
        if error_files_count > 0:
            print(f"Encountered errors in {error_files_count} files.")
        print(f"Output written to: {output_file}")

    except IOError as e:
        print(f"Error opening or writing to output file {output_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred during merging: {e}", file=sys.stderr)


def main():
    args = parse_arguments()
    merge_json_files(args.input_dir, args.output_file)


if __name__ == "__main__":
    main()
