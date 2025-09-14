# arXiv Preprint Matching Strategy

This is the code repository for the COMET initiative pilot project: 'Match Preprints to Published Journal Articles'. Refer to the [Project Hub](https://docs.google.com/document/d/1oQ2VUdRz2affr2AnogmLx4hjOGf0SldkGVI0n7HIFBE/edit?usp=sharing) for a full description of the project and information on getting involved. 

This strategy is a modified form of [Search Based Matching with Validation (SBMV) preprint matching strategy](https://gitlab.com/crossref/labs/marple/-/blob/main/strategies_available/preprint_sbmv/strategy.py?ref_type=heads) developed by [@dtkaczyk](https://github.com/dtkaczyk), specifically adapted for matching arXiv preprint DOIs represented in the DataCite schema.

If you use this strategy, please cite: 

Buttrick, Adam. "arXiv preprint to publication matching strategy." Collaborative Metadata (COMET), 2025, DOI: [10.82461/S678-CV26](https://doi.org/10.82461/S678-CV26).


## Description of Strategy

This strategy attempts to find potential published versions (primarily journal articles) corresponding to input preprint records (expected in DataCite JSON format). It uses the Crossref API and applies scoring based on metadata similarity.


### Search Approach and Candidate Filtering

1.  A bibliographic query string is built using metadata extracted from the DataCite input: the main title (and subtitle, if present), publication year, and the family names of personal authors listed as `creators` or `contributors`. These components are normalized using `unidecode`, lowercasing, and removing punctuation before constructing the query.
2.  The query targets the Crossref `/works` endpoint via a robust HTTP session, using the `query.bibliographic` parameter and returning up to 25 candidates (`rows=25`). The maximum query length is capped (default 5000, adjustable via `--max-query-len`).
3.  Candidates retrieved from Crossref are filtered based on their work type (`type` field) to include only relevant publication types such as `journal-article`, `proceedings-article`, `book-chapter`, `report`, and `posted-content`.

### Scoring Logic, Weights, and Heuristics:

The strategy employs weighted scoring based on year, title, and author similarity, incorporating fuzzy matching and heuristics:

* **Year Score:**
    * Compares the preprint's `publicationYear` with the candidate's publication year (extracted carefully from fields like `published-online`, `published-print`, `issued`, `created`).
    * Assigns scores based on the difference (`candidate_year - preprint_year`): 1.0 for diff 0-2; 0.9 for diff 3; 0.8 for diff 4; 0.0 otherwise (penalizing cases where candidate significantly predates or postdates the preprint). Returns 0.0 if years cannot be compared.
* **Title Score:**
    * Compares normalized titles (input vs. candidate). Normalization includes Unicode handling, accent removal, lowercasing, and punctuation stripping.
    * Uses a weighted blend of fuzzy matching scores: `0.4 * fuzz.token_set_ratio + 0.4 * fuzz.token_sort_ratio + 0.2 * fuzz.WRatio`.
    * Applies a penalty (`*= 0.7`) if the *first normalized word* of one title contains keywords like "correction", "reply", "erratum", etc., while the other title does not.
* **Author Score:**
    * Applies several heuristics for comparing normalized author lists:
        * An exact match between valid, normalized ORCIDs results in a score of 1.0; a mismatch results in 0.0, skipping name comparison.
        * Iteratively finds the most similar pair of authors between the two lists using `_score_normalized_author_similarity`. This comparison uses `fuzz.token_sort_ratio` on pre-calculated, normalized name variations (e.g., "J Smith", "Smith J", "John Smith", "Smith John").
        * Author pairs with a similarity below 0.5 are discarded during the greedy matching.
        * If family names match *and* the name similarity score is > 0.6, the pair's score is boosted slightly (`* 1.1`).
        * For efficiency, compares sorted strings of normalized family names using `fuzz.token_sort_ratio`.
        * The final author score (based on the sum of matched pair scores in the greedy approach) is normalized by the total number of unique authors involved: `(2.0 * score_sum) / total_authors`, clamped between 0.0 and 1.0. Handles empty lists gracefully.
* **Final Weighted Score:** Calculated as: `(weight_year * year_score + weight_title * title_score + weight_author * author_score) / (weight_year + weight_title + weight_author)`. Default weights are `weight_title=2.0`, `weight_author=0.8`, `weight_year=0.4`. These weights can be adjusted via command-line arguments.

### Match Selection

1.  Only candidates achieving a final weighted score >= `min_score` (default 0.85) are considered potential matches.
2.  Among these, only candidates whose scores are within `max_score_diff` (default 0.03) of the *highest* score obtained for that input record are returned as the final match(es). This helps select the best result(s) when multiple candidates have very similar high scores.

### ColBERT Reranking (Optional)

When enabled with `--enable-reranker`, the strategy incorporates a neural reranking step using Pylate's ColBERT implementation to improve matching accuracy:

#### How it Works:
1. The traditional heuristic-based scoring (year, title, author) is performed first on all candidates retrieved from Crossref.
2. For reranking, text representations are created by combining titles and author names from both the input preprint and candidate matches.
3. The ColBERT model generates dense embeddings for both the query (input preprint) and candidate documents.
4. The model computes similarity scores between the query and each candidate using late interaction (MaxSim) between token embeddings.
5. Reranker scores are normalized to [0, 1] range using min-max normalization across all candidates.
6. Final scores combine both heuristic and reranker scores using configurable weights:
   - Final Score = (`heuristic_weight` × heuristic_score) + (`reranker_weight` × reranker_score)
   - Default weights: 0.3 for heuristic, 0.7 for reranker


#### Configuration:
- Model: Default uses `lightonai/GTE-ModernColBERT-v1`, but any ColBERT-compatible model can be specified
- Batch size: Adjust `--reranker-batch-size` based on available memory (default: 16)
- Weighting: Tune `--heuristic-weight` and `--reranker-weight` to balance traditional and neural scoring

#### Fallback Behavior:
If reranking fails (missing dependencies, model loading errors, or runtime exceptions), the system automatically falls back to heuristic-only scoring with appropriate logging.
