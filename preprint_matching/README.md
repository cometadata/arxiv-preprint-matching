# Preprint Matching Tool

Modified form of [Search Based Matching with Validation (SBMV) preprint matching strategy](https://gitlab.com/crossref/labs/marple/-/blob/main/strategies_available/preprint_sbmv/strategy.py?ref_type=heads) developed by [@dtkaczyk](https://github.com/dtkaczyk), specifically adapted for matching arXiv preprint DOIs represented in the DataCite schema.

## Installation

```
pip install -r requirements.txt
```

## Usage

```
python preprint_match_data_files.py -i INPUT_FILE -o OUTPUT_FILE -f FORMAT -m EMAIL -u USER_AGENT [-ll LOG_LEVEL] [-lf LOG_FILE] [-lc] [-cf CANDIDATE_LOG_FILE]
```

### Required Arguments
- `-i, --input`: Path to input JSONL or gzipped JSONL (.jsonl.gz) file
- `-o, --output`: Path to output file (CSV or JSON)
- `-f, --format`: Output format ('json' or 'csv')
- `-m, --mailto`: Email address for Crossref API politeness
- `-u, --user-agent`: User-Agent string for API requests

### Optional Arguments
- `-ll, --log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL, NONE)
- `-lf, --log-file`: Path to log file (defaults to stderr)
- `-lc, --log-candidates`: If set, logs raw Crossref candidate results
- `-cf, --candidate-log-file`: Path for logging candidates (default: crossref_candidates.log)


## Examples

Process a JSONL file and output matches to CSV:
```
python preprint_match_data_files.py -i preprints.jsonl -o matches.csv -f csv -m user@example.com -u "PrerintMatchingTool/1.0"
```

Process a gzipped JSONL file with detailed logging:
```
python preprint_match_data_files.py -i preprints.jsonl.gz -o matches.json -f json -m user@example.com -u "PrerintMatchingTool/1.0" -ll DEBUG -lf matching.log -lc
```

## Description of Strategy


### Search Approach and Candidate Filtering

1.  A bibliographic query string is built using metadata extracted from the DataCite input: the main title (and subtitle, if present), publication year, and the family names of personal authors listed as `creators` or `contributors`. These components are then normalized using a more thorough `_normalize_string` function (handling Unicode, accents, case, punctuation) before query construction.
2. The then query targets the Crossref `/works` endpoint using the `query.bibliographic` parameter, requesting up to 25 results (`rows=25`).
3. Since we're inverting the search to begin with preprints, instead of pre-filter the query, the strategy retrieves a broader set of candidates and then filters them after retrieval. We retains candidates whose Crossref work type matches a predefined list (`accepted_crossref_types`), which includes `journal-article`, `proceedings-article`, `book-chapter`, `report`, and `posted-content`. 


### Scoring Logic, Weights, and Heuristics:

The core logic and primary changes to the the strategy (as comparared to the original), lie in the use of some new weighted scoring mechanisms and leveraging a blend of fuzzy matches:

* **Year Score:**
    * Calculates `preprint_year - article_year`.
    * **Heuristic:** Assigns scores based on this difference: 1.0 if diff is 0-2; 0.9 if diff is 3; 0.8 if diff is 4; 0.0 otherwise (preprint should not significantly predate published version). Uses robust multi-field date extraction for candidates.
* **Title Score:**
    * Compares normalized titles by:
       * **Heuristic:** Uses a weighted blend of fuzzy matching scores: `0.45 * fuzz.token_set_ratio + 0.45 * fuzz.token_sort_ratio + 0.10 * fuzz.ratio`.
       * **Heuristic:** Applies a penalty (`*= 0.67`) if the first three normalized words of one title contain keywords like "correction", "reply", "erratum", etc., while the other title does not.
* **Author Score:** 
   * This employs several heuristics:
       * **ORCID Priority:** If both authors have valid, normalized ORCIDs, a match gives 1.0, a mismatch gives 0.0, bypassing name comparison.
       * **Large List Heuristic (Total Authors > 50):** Compares the space-separated, sorted strings of normalized family names from each list using `fuzz.token_sort_ratio`.
       * **Pairwise Greedy Matching (Smaller Lists):** Iteratively finds the most similar pair of authors (one from each list) using `_score_normalized_author_similarity`. This function compares pre-calculated, normalized name variations using `fuzz.token_sort_ratio`.
       * **Pair Match Threshold:** Pairs below a similarity of 0.5 in the greedy match step are ignored.
       * **Family Name Boost Heuristic:** If family names match and the name similarity score is > 0.6, the pair's score is boosted slightly (`* 1.1`).
       * **Normalization:** The sum of scores from matched pairs is normalized by the total number of authors in both lists: `(2.0 * score_sum) / total_authors`, clamped between 0.0 and 1.0.
* **Weighted Average:** The final score is calculated as `(0.4 * year_score + 1.0 * title_score + 2.2 * author_score) / 3.6`. The weighting heavily emphasize author similarity (`2.2`) over title (`1.0`) and year (`0.4`).

### Match Selection

1. Only candidates achieving a final weighted score >= `min_score` (0.85) are considered potential matches.
2. Among these, only the candidates whose scores are within `max_score_diff` (0.04) of the highest score obtained are returned as the final matches. This selects the best match(es) when scores are very close.
