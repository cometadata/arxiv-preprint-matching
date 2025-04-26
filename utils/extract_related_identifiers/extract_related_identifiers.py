import argparse
import json
import os
import sys
import csv

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Extract main and related DOIs from JSON files into a CSV."
    )
    parser.add_argument(
        "-i", "--input_dir",
        required=True,
        help="Path to the directory containing input JSON files."
    )
    parser.add_argument(
        "-o", "--output_file",
        required=True,
        help="Path to the output CSV file."
    )
    return parser.parse_args()

def extract_dois_from_file(file_path):
    main_doi = None
    related_dois = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        try:
            main_doi = data.get('attributes', {}).get('doi')
        except AttributeError:
             print(f"Warning: Unexpected structure for 'attributes' in {file_path}", file=sys.stderr)

        try:
            related_identifiers = data.get('attributes', {}).get('relatedIdentifiers')
            if isinstance(related_identifiers, list):
                for item in related_identifiers:
                    if isinstance(item, dict):
                        id_type = item.get('relatedIdentifierType')
                        identifier = item.get('relatedIdentifier')
                        if id_type and id_type.upper() == 'DOI' and identifier:
                            related_dois.append(identifier)
        except AttributeError:
             print(f"Warning: Unexpected structure for 'relatedIdentifiers' in {file_path}", file=sys.stderr)

    except FileNotFoundError:
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None, None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {file_path}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"Error processing file {file_path}: {e}", file=sys.stderr)
        return None, None

    return main_doi, related_dois


def process_directory(input_dir, output_file):
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory not found or is not a directory: {input_dir}", file=sys.stderr)
        return

    processed_files_count = 0
    error_files_count = 0
    found_dois_count = 0

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(['filename', 'doi', 'related_doi'])

            for root, _, files in os.walk(input_dir):
                for filename in files:
                    if filename.lower().endswith(".json"):
                        file_path = os.path.join(root, filename)
                        print(f"Processing: {file_path}")

                        main_doi, related_dois = extract_dois_from_file(file_path)

                        if main_doi is not None or related_dois:
                            csv_writer.writerow([filename, main_doi if main_doi else '', ','.join(related_dois)])
                            processed_files_count += 1
                            if main_doi or related_dois:
                                found_dois_count +=1
                        elif main_doi is None and not related_dois and os.path.exists(file_path):
                            csv_writer.writerow([filename, '', ''])
                            processed_files_count += 1
                        else:
                            error_files_count += 1


        print(f"\nProcessing complete.")
        print(f"Processed {processed_files_count} JSON files.")
        print(f"Found DOI information in {found_dois_count} files.")
        if error_files_count > 0:
            print(f"Encountered errors processing {error_files_count} files (check logs above).")
        print(f"Output written to: {output_file}")

    except IOError as e:
        print(f"Error opening or writing to output file {output_file}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}", file=sys.stderr)


def main():
    args = parse_arguments()
    process_directory(args.input_dir, args.output_file)

if __name__ == "__main__":
    main()