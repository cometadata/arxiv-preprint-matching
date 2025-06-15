import csv
import sys
import argparse
import requests


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Finding missing type values for DOIs using the Crossref API"
    )
    parser.add_argument("-i", "--input", required=True,
                        help="Path to the input CSV file.")
    parser.add_argument("-o", "--output", required=True,
                        help="Path for the output CSV file.")
    parser.add_argument("-m", "--mailto", required=True,
                        help="Email address for accessing the Crossref polite pool.")
    parser.add_argument("-u", "--user-agent", default="Retrieve-DOI-Type/1.0",
                        help="Custom User-Agent string for the API request.")
    args, _ = parser.parse_known_args()
    return args


def get_crossref_type(doi, mailto, user_agent):
    if not doi:
        return 'no_doi_provided'
    url = f"https://api.crossref.org/works/{doi}?mailto={mailto}"
    headers = {'User-Agent': user_agent}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('message', {}).get('type', 'type_not_found')
    except requests.exceptions.RequestException as e:
        print(f"Error querying Crossref for DOI {doi}: {e}", file=sys.stderr)
        return 'api_error'


def process_csv_file(input_path, output_path, mailto, user_agent):
    try:
        with open(input_path, mode='r', encoding='utf-8') as f_in, \
                open(output_path, mode='w', encoding='utf-8') as f_out:

            reader = csv.reader(f_in)
            writer = csv.writer(f_out)

            header = next(reader)
            writer.writerow(header)

            try:
                matched_doi_idx = header.index('matched_doi')
                type_idx = header.index('matched_doi_type')
            except ValueError as e:
                print(f"Error: Missing expected column in CSV header - {e}", file=sys.stderr)
                return

            print("Processing CSV...")
            for row in reader:
                if len(row) <= type_idx:
                    writer.writerow(row)
                    continue

                if not row[type_idx].strip():
                    matched_doi = row[matched_doi_idx].strip()
                    print(f"Querying Crossref for DOI: {matched_doi}")
                    new_type = get_crossref_type(
                        matched_doi, mailto, user_agent)
                    row[type_idx] = new_type

                writer.writerow(row)

            print(f"Processing complete. Output written to {output_path}")

    except FileNotFoundError:
        print(f"Error: The file '{input_path}' was not found.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)


def main():
    args = parse_arguments()
    process_csv_file(args.input, args.output, args.mailto, args.user_agent)


if __name__ == "__main__":
    main()
