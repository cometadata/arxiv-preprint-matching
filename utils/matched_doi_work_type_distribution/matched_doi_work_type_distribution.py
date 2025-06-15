import pandas as pd
import argparse


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Calculate the distribution of matched_doi_type values.")
    parser.add_argument("-i", "--input", required=True,
                        help="Path to the input CSV file.")
    parser.add_argument("-o", "--output", required=True,
                        help="Path to the output CSV file.")
    return parser.parse_args()


def calculate_and_save_distribution(input_path, output_path):
    try:
        df = pd.read_csv(input_path)

        counts = df['matched_doi_type'].value_counts(dropna=False)
        percentages = df['matched_doi_type'].value_counts(
            dropna=False, normalize=True) * 100

        distribution_df = pd.DataFrame({
            'count': counts,
            'percentage': percentages
        })

        distribution_df = distribution_df.rename(index={pd.NA: 'null'})

        distribution_df['percentage'] = distribution_df['percentage'].map(
            '{:.2f}%'.format)

        print("Distribution of matched_doi_type:")
        print(distribution_df)

        distribution_df.to_csv(output_path, index_label='matched_doi_type')
        print(f"\nDistribution successfully saved to {output_path}")

    except FileNotFoundError:
        print(f"Error: The file '{input_path}' was not found.")
    except KeyError:
        print(f"Error: The input CSV must contain a 'matched_doi_type' column.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    args = parse_arguments()
    calculate_and_save_distribution(args.input, args.output)


if __name__ == "__main__":
    main()
