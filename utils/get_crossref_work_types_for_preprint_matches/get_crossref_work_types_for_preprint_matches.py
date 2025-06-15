import csv
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-r", "--reference", required=True)
    parser.add_argument(
        "-o", "--output", default='preprint_matches_w_work_types.csv')
    return parser.parse_args()


def load_reference_data(reference_file):
    doi_to_type_map = {}
    with open(reference_file, 'r', encoding='utf-8') as ref_file:
        reader = csv.DictReader(ref_file)
        for row in reader:
            if row.get('field_name') == 'type':
                doi = row.get('doi')
                doi_type = row.get('value')
                if doi and doi_type:
                    doi_to_type_map[doi] = doi_type
    return doi_to_type_map


def process_input_data(input_file, output_file, doi_to_type_map):
    with open(input_file, 'r', encoding='utf-8') as in_file, \
            open(output_file, 'w', encoding='utf-8') as out_file:
        reader = csv.DictReader(in_file)
        output_fieldnames = reader.fieldnames + ['matched_doi_type']
        writer = csv.DictWriter(out_file, fieldnames=output_fieldnames)
        writer.writeheader()
        for row in reader:
            matched_doi = row.get('matched_doi')
            doi_type = doi_to_type_map.get(matched_doi, '')
            row['matched_doi_type'] = doi_type
            writer.writerow(row)


def main():
    args = parse_arguments()
    doi_type_map = load_reference_data(args.reference)
    process_input_data(args.input, args.output, doi_type_map)


if __name__ == "__main__":
    main()
