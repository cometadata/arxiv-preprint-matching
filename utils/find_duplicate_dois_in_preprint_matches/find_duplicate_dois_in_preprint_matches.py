import csv
import argparse
from collections import defaultdict


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Find and report duplicate matched DOIs in a CSV file."
    )
    parser.add_argument(
        "-i",  "--input",  required=True, help="Input CSV file path."
    )
    parser.add_argument(
        "-o",  "--output",  default="duplicates.csv", help="Output CSV file path for duplicate rows. (Default: duplicates.csv)"
    )
    parser.add_argument(
        "-r",  "--report",  default="report.txt", help="Output file for the summary report. (Default: report.txt)"
    )
    return parser.parse_args()


def find_and_report_duplicates(input_file, output_csv_file, report_file):
    try:
        with open(input_file, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)

            try:
                doi_column_index = header.index('matched_doi')
            except ValueError:
                print("Error: The input file must contain a 'matched_doi' column.")
                return

            all_rows = list(reader)
            doi_counts = defaultdict(int)

            for row in all_rows:
                if len(row) > doi_column_index:
                    doi = row[doi_column_index]
                    doi_counts[doi] += 1

        duplicate_dois_set = {doi for doi,
                              count in doi_counts.items() if count > 1}

        output_rows = [
            row for row in all_rows
            if len(row) > doi_column_index and row[doi_column_index] in duplicate_dois_set
        ]

        if not duplicate_dois_set:
            report_content = "No duplicate matched_doi values were found."
        else:
            report_content = (
                f"Found {len(duplicate_dois_set)} unique DOIs with multiple matches.\n"
                f"Total rows with these DOIs: {len(output_rows)}.\n\n"
            )
        try:
            with open(report_file, 'w', encoding='utf-8') as f_report:
                f_report.write(report_content)
            print(f"Report successfully saved to {report_file}")
        except IOError as e:
            print(f"Error writing report file: {e}")

        with open(output_csv_file, 'w', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            if output_rows:
                writer.writerows(output_rows)

        print(f"CSV with duplicate rows saved to {output_csv_file}")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    args = parse_arguments()
    find_and_report_duplicates(args.input, args.output, args.report)


if __name__ == "__main__":
    main()
