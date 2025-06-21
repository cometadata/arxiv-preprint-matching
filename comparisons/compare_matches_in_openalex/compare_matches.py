import re
import csv
import sys
import time
import logging
import argparse
import requests
from urllib.parse import quote


def parse_args():
    parser = argparse.ArgumentParser(
        description='Compare arXiv preprint matches with OpenAlex locations on the matched DOIs to identify overlap')
    parser.add_argument('-i', '--input', required=True,
                        help='Input CSV file path')
    parser.add_argument('-o', '--output', required=True,
                        help='Output CSV file path')
    parser.add_argument('-m', '--mailto', 
                        help='Email address for OpenAlex polite pool access')
    parser.add_argument('-l', '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default='INFO', help='Set logging level (default: INFO)')
    return parser.parse_args()


def extract_arxiv_id(arxiv_doi):
    logging.debug(f"Extracting arXiv ID from DOI: {arxiv_doi}")
    
    # Current format arXiv IDs: YYMM.NNNN or YYMM.NNNNN
    match = re.search(r'arxiv\.(\d{4}\.\d{4,5})', arxiv_doi.lower())
    if match:
        arxiv_id = match.group(1)
        logging.debug(f"Extracted arXiv ID (new format): {arxiv_id}")
        return arxiv_id
    
    # Older format arXiv IDs: subject-class/YYMMnnn
    match = re.search(r'arxiv\.([a-z-]+/\d{7})', arxiv_doi.lower())
    if match:
        arxiv_id = match.group(1)
        logging.debug(f"Extracted arXiv ID (old format): {arxiv_id}")
        return arxiv_id
    
    logging.warning(f"Could not extract arXiv ID from DOI: {arxiv_doi}")
    return None


def query_openalex(doi, mailto=None, max_retries=3):
    url = f"https://api.openalex.org/works/https://doi.org/{quote(doi)}"
    if mailto:
        url += f"?mailto={mailto}"
    
    logging.debug(f"Querying OpenAlex for DOI: {doi}")
    logging.debug(f"Request URL: {url}")
    
    for attempt in range(max_retries):
        try:
            logging.debug(f"Attempt {attempt + 1}/{max_retries} for DOI: {doi}")
            response = requests.get(url, timeout=30)
            logging.debug(f"Response status code: {response.status_code} for DOI: {doi}")
            
            if response.status_code == 200:
                data = response.json()
                logging.debug(f"Successfully retrieved data for DOI: {doi}")
                return data
            elif response.status_code == 429:
                wait_time = 2 ** attempt
                logging.warning(f"Rate limited (429) for DOI: {doi}, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    logging.info(f"Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"Rate limit exceeded after {max_retries} attempts for DOI: {doi}")
                    return None
            elif response.status_code == 404:
                logging.warning(f"DOI not found in OpenAlex (404): {doi}")
                return None
            elif response.status_code == 400:
                logging.error(f"Bad request (400) for DOI: {doi}. URL: {url}")
                return None
            else:
                logging.error(f"Unexpected status code {response.status_code} for DOI: {doi}")
                return None
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout on attempt {attempt + 1}/{max_retries} for DOI: {doi}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                logging.error(f"Timeout after {max_retries} attempts for DOI: {doi}")
                return None
        except requests.exceptions.ConnectionError as e:
            logging.warning(f"Connection error on attempt {attempt + 1}/{max_retries} for DOI: {doi}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                logging.error(f"Connection failed after {max_retries} attempts for DOI: {doi}")
                return None
        except requests.RequestException as e:
            logging.warning(f"Request exception on attempt {attempt + 1}/{max_retries} for DOI: {doi}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                logging.error(f"Request failed after {max_retries} attempts for DOI: {doi}: {str(e)}")
                return None
    return None


def check_arxiv_in_locations(locations, arxiv_id):
    logging.debug(f"Checking for arXiv ID {arxiv_id} in {len(locations) if locations else 0} locations")
    
    if not locations or not arxiv_id:
        logging.debug("No locations or no arXiv ID provided")
        return False

    for i, location in enumerate(locations):
        logging.debug(f"Checking location {i + 1}: {location}")
        
        landing_url = location.get('landing_page_url', '')
        if landing_url and 'arxiv' in landing_url.lower() and arxiv_id in landing_url:
            logging.info(f"Found arXiv match in landing page URL: {landing_url}")
            return True

        pdf_url = location.get('pdf_url', '')
        if pdf_url and 'arxiv' in pdf_url.lower() and arxiv_id in pdf_url:
            logging.info(f"Found arXiv match in PDF URL: {pdf_url}")
            return True

    logging.debug(f"No arXiv matches found for ID {arxiv_id} in any location")
    return False


def process_csv(input_file, output_file, mailto=None):
    logging.info(f"Starting processing of {input_file}")
    logging.info(f"Output will be written to {output_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f_in:
        reader = csv.DictReader(f_in)
        rows = list(reader)
        total_rows = len(rows)
        
        logging.info(f"Total rows to process: {total_rows}")
        fieldnames = reader.fieldnames + ['matched_in_openalex']

        with open(output_file, 'w', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            
            error_count = 0
            success_count = 0
            match_count = 0

            for i, row in enumerate(rows, 1):
                input_doi = row['input_doi']
                matched_doi = row['matched_doi']
                
                logging.info(f"Processing row {i}/{total_rows}: input_doi={input_doi}, matched_doi={matched_doi}")
                
                arxiv_id = extract_arxiv_id(input_doi)
                
                if i % 50 == 0:
                    print(f"Progress: {i}/{total_rows} ({i/total_rows*100:.1f}%) - Errors: {error_count}, Matches: {match_count}")
                
                if not arxiv_id:
                    logging.error(f"Failed to extract arXiv ID from input DOI: {input_doi}")
                    row['matched_in_openalex'] = 'ERROR'
                    error_count += 1
                    writer.writerow(row)
                    f_out.flush()
                    continue

                time.sleep(0.5)
                
                openalex_data = query_openalex(matched_doi, mailto)

                if openalex_data is None:
                    logging.error(f"Failed to retrieve OpenAlex data for matched DOI: {matched_doi}")
                    row['matched_in_openalex'] = 'ERROR'
                    error_count += 1
                    writer.writerow(row)
                    f_out.flush()
                    continue

                locations = openalex_data.get('locations', [])
                logging.debug(f"Retrieved {len(locations)} locations for DOI: {matched_doi}")
                
                has_arxiv_link = check_arxiv_in_locations(locations, arxiv_id)

                result = 'TRUE' if has_arxiv_link else 'FALSE'
                row['matched_in_openalex'] = result
                
                if has_arxiv_link:
                    match_count += 1
                    logging.info(f"Row {i}: MATCH found for arXiv ID {arxiv_id} in OpenAlex DOI {matched_doi}")
                else:
                    logging.info(f"Row {i}: NO MATCH for arXiv ID {arxiv_id} in OpenAlex DOI {matched_doi}")
                
                success_count += 1
                writer.writerow(row)
                f_out.flush()
            
            logging.info(f"Processing complete. Total: {total_rows}, Successful: {success_count}, Errors: {error_count}, Matches: {match_count}")


def main():
    args = parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.info(f"Starting compare_matches.py with log level: {args.log_level}")
    logging.info(f"Input file: {args.input}")
    logging.info(f"Output file: {args.output}")
    
    try:
        if args.mailto:
            logging.info(f"Using mailto parameter for polite pool: {args.mailto}")
            print(f"Using mailto parameter for polite pool: {args.mailto}")
        
        process_csv(args.input, args.output, args.mailto)
        print(f"Processing complete. Output written to {args.output}")
        logging.info("Script completed successfully")
        
    except FileNotFoundError:
        error_msg = f"Error: Input file '{args.input}' not found."
        logging.error(error_msg)
        print(error_msg, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        logging.error(error_msg, exc_info=True)
        print(error_msg, file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
