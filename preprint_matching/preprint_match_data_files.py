import csv
import sys
import gzip
import json
import logging
import argparse
from urllib.parse import urlparse
from strategies.preprint_sbmv_datacite.strategy import PreprintSbmvStrategy


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Process JSONL input to find preprint matches, outputting input/matched DOIs to CSV or a single JSON object."
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Path to the input JSONL or gzipped JSONL (.jsonl.gz) file."
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Path to the output file (CSV or JSON)."
    )
    parser.add_argument(
        "-f", "--format",
        required=True,
        choices=['json', 'csv'],
        help="Output format ('json' for a single JSON object, 'csv' for CSV)."
    )
    parser.add_argument(
        "-m", "--mailto",
        required=True,
        help="Email address for Crossref API politeness."
    )
    parser.add_argument(
        "-u", "--user-agent",
        required=True,
        help="User-Agent string for Crossref API requests."
    )
    parser.add_argument(
        "-ll", "--log-level",
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NONE'],
        help="Set the logging level for detailed output (default: INFO). 'NONE' disables strategy logging."
    )
    parser.add_argument(
        "-lf", "--log-file",
        help="Optional: Path to a file to write logs to. If not provided, logs go to stderr."
    )
    parser.add_argument(
        "-lc", "--log-candidates",
        action='store_true',
        help="If set, log raw Crossref candidate results to the candidate log file."
    )
    parser.add_argument(
        "-cf", "--candidate-log-file",
        default="crossref_candidates.log",
        help="Path to the file for logging raw candidates (default: crossref_candidates.log)."
    )
    return parser.parse_args()


def extract_doi_from_url(url_string):
    if not url_string or not isinstance(url_string, str):
        return None
    try:
        parsed = urlparse(url_string)
        if parsed.netloc and parsed.netloc.lower() == 'doi.org':
            return parsed.path.lstrip('/')
    except Exception as e:
        logging.warning(f"Could not parse URL '{url_string}' to extract DOI: {e}")
        pass
    return None


def setup_logging(log_level_str, log_file=None):
    numeric_level = getattr(logging, log_level_str.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Warning: Invalid log level '{log_level_str}'. Defaulting to INFO.", file=sys.stderr)
        numeric_level = logging.INFO
        log_level_str = 'INFO'

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    formatter = logging.Formatter(log_format, datefmt=date_format)

    root_logger = logging.getLogger()

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if log_level_str.upper() == 'NONE':
        root_logger.setLevel(logging.CRITICAL + 1)
        print("Logging disabled ('NONE' selected).")
        return

    root_logger.setLevel(numeric_level)

    if log_file:
        print(f"Logging to file: {log_file} at level: {log_level_str.upper()}")
        handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    else:
        print(f"Logging to stderr at level: {log_level_str.upper()}")
        handler = logging.StreamHandler(sys.stderr)

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    if root_logger.hasHandlers():
        logging.info("Logging configured successfully.")
    elif log_level_str.upper() != 'NONE':
        print("Warning: Logging setup completed, but no handlers seem to be attached.", file=sys.stderr)


def process_jsonl_file(input_path, output_path, output_format, mailto, user_agent, log_level, log_candidates_flag, candidate_log_filename):

    main_logger = logging.getLogger(__name__)

    try:
        strategy_logger = logging.getLogger('strategy')

        matching_strategy = PreprintSbmvStrategy(
            mailto=mailto,
            user_agent=user_agent,
            logger_instance=strategy_logger,
            log_candidates=log_candidates_flag,
            candidate_log_file=candidate_log_filename
        )
        main_logger.info("Preprint matching strategy initialized.")
        if log_candidates_flag:
            main_logger.info(f"Strategy configured to log candidates to: {candidate_log_filename}")

    except ValueError as e:
        main_logger.critical(f"Fatal Error: Could not initialize strategy: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        main_logger.critical(f"Fatal Error: Unexpected issue initializing strategy: {e}", exc_info=True)
        sys.exit(1)

    main_logger.info(f"Starting processing of file: {input_path}")
    if output_format == 'json':
        main_logger.info(f"Output format: JSON. Writing to: {output_path}")
    else:
        main_logger.info(f"Output format: CSV. Writing to: {output_path}")

    is_gzipped = input_path.lower().endswith('.gz')
    open_func = gzip.open if is_gzipped else open
    read_mode = 'rt'
    write_mode = 'wt'

    all_results_json = []
    processed_lines = 0
    matched_lines = 0
    errors_encountered = 0
    writer = None

    try:
        with open_func(input_path, read_mode, encoding='utf-8') as infile, \
                open(output_path, write_mode, encoding='utf-8') as outfile:

            if output_format == 'csv':
                fieldnames = ['input_doi', 'matched_doi']
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                main_logger.debug("CSV writer initialized with header.")

            for i, line in enumerate(infile):
                line_num = i + 1
                line = line.strip()
                if not line:
                    main_logger.debug(f"Skipping empty line {line_num}.")
                    continue

                main_logger.debug(f"--- Processing Line {line_num} ---")
                input_doi = "N/A"
                matched_doi_url = None
                matched_doi = None
                output_record = None
                line_processed_successfully = False

                try:
                    try:
                        input_data = json.loads(line)
                        if isinstance(input_data, dict):
                            input_doi = input_data.get(
                                'id') or input_data.get('doi')
                            if not input_doi:
                                main_logger.warning(f"Line {line_num}: Input JSON lacks 'id' or 'doi' field.")
                                input_doi = "N/A_MISSING_ID"
                            else:
                                main_logger.debug(f"Line {line_num}: Input DOI identified as '{input_doi}' from JSON.")
                        else:
                            main_logger.warning(f"Line {line_num}: Parsed JSON is not a dictionary (type: {type(input_data)}). Cannot reliably get input DOI.")
                            input_doi = "N/A_INVALID_JSON_TYPE"

                    except json.JSONDecodeError as e:
                        main_logger.error(f"Line {line_num}: JSON decode error: {e}. Raw line (start): '{line[:100]}...'")
                        errors_encountered += 1
                        continue

                    matches = matching_strategy.match(line)

                    if matches and isinstance(matches, list) and len(matches) > 0:
                        first_match = matches[0]
                        if isinstance(first_match, dict):
                            matched_doi_url = first_match.get('id')
                            confidence = first_match.get('confidence', 'N/A')
                            conf_str = f"{confidence:.4f}" if isinstance(confidence, (int, float)) else str(confidence)
                            main_logger.info(f"Line {line_num} (Input DOI {input_doi}): Found match '{matched_doi_url}' with confidence {conf_str}")
                            matched_lines += 1
                        else:
                            main_logger.warning(f"Line {line_num} (Input DOI {input_doi}): Match result item is not a dictionary: {type(first_match)}")

                    else:
                        main_logger.info(f"Line {line_num} (Input DOI {input_doi}): No preprint match found by strategy.")

                    if matched_doi_url:
                        matched_doi = extract_doi_from_url(matched_doi_url)
                        if not matched_doi:
                            main_logger.warning(f"Line {line_num} (Input DOI {input_doi}): Could not extract DOI from matched URL '{matched_doi_url}'")

                    output_record = {
                        "input_doi": input_doi if input_doi and not input_doi.startswith("N/A") else '',
                        "matched_doi": matched_doi if matched_doi else ''
                    }
                    line_processed_successfully = True

                except Exception as e:
                    main_logger.error(f"Line {line_num} (Input DOI {input_doi}): Unexpected error during processing: {e}", exc_info=True)
                    errors_encountered += 1

                if line_processed_successfully and output_record is not None:
                    if output_format == 'json':
                        all_results_json.append(output_record)
                        main_logger.debug(f"Line {line_num}: Appended result to JSON list.")
                    elif writer:
                        try:
                            writer.writerow(output_record)
                            main_logger.debug(f"Line {line_num}: Wrote result row to CSV.")
                        except Exception as e:
                            main_logger.error(f"Line {line_num}: Failed to write row to CSV: {e}. Data: {output_record}", exc_info=True)
                            errors_encountered += 1

                processed_lines += 1
                if processed_lines % 100 == 0:
                    main_logger.info(f"Progress: Processed {processed_lines} lines...")

            if output_format == 'json':
                main_logger.info(f"Writing {len(all_results_json)} collected results as JSON to {output_path}...")
                try:
                    json.dump(all_results_json, outfile,
                              ensure_ascii=False, indent=2)
                    main_logger.info("JSON writing complete.")
                except Exception as e:
                    main_logger.critical(f"Fatal Error: Failed to write JSON output to file: {e}", exc_info=True)

    except FileNotFoundError:
        main_logger.critical(f"Fatal Error: Input file not found at '{input_path}'")
        sys.exit(1)
    except gzip.BadGzipFile:
        main_logger.critical(f"Fatal Error: Input file '{input_path}' is corrupted or not a valid gzip file.")
        sys.exit(1)
    except IOError as e:
        main_logger.critical(f"Fatal Error: File I/O error accessing input '{input_path}' or output '{output_path}': {e}", exc_info=True)
        sys.exit(1)
    except ImportError as e:
        main_logger.critical(f"Fatal Error: ImportError: {e}. Ensure strategy module and dependencies are available.", exc_info=True)
        sys.exit(1)
    except Exception as e:
        main_logger.critical(f"Fatal Error: An unexpected error occurred during file processing: {e}", exc_info=True)
        sys.exit(1)

    main_logger.info("--- Processing Summary ---")
    main_logger.info(f"Total lines processed from input: {processed_lines}")
    main_logger.info(f"Lines resulting in a preprint match: {matched_lines}")
    if errors_encountered > 0:
        main_logger.warning(f"Lines skipped or failed due to errors: {errors_encountered}")
    else:
        main_logger.info("No errors encountered during line processing.")
    main_logger.info(f"Results written to: {output_path} (Format: {output_format.upper()})")
    main_logger.info("Processing complete.")


def main():
    args = parse_arguments()

    setup_logging(args.log_level, args.log_file)

    process_jsonl_file(
        args.input,
        args.output,
        args.format,
        args.mailto,
        args.user_agent,
        args.log_level,
        args.log_candidates,
        args.candidate_log_file
    )


if __name__ == "__main__":
    main()
