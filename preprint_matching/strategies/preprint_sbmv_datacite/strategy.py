import re
import json
import logging
import datetime
import unicodedata
import urllib.parse
import requests
from rapidfuzz import fuzz
from unidecode import unidecode
from matching.utils import crossref_rest_api_call, doi_id


class PreprintSbmvStrategy:
    strategy = "preprint-sbmv-datacite-contrib-v2"
    task = "preprint-matching"
    description = (
        "This strategy uses Crossref's REST API to search for candidate "
        + "preprints based on metadata (title, creators, contributors, year) "
        + "extracted from a DataCite JSON input, and compares the normalized metadata "
        + "to validate the candidates using enhanced scoring."
    )
    default = False

    min_score = 0.85
    max_score_diff = 0.04
    max_query_len = 5000

    weight_year = 0.4
    weight_title = 1.0
    weight_author = 2.2

    accepted_crossref_types = [
        "journal-article",
        "proceedings-article",
        "book-chapter",
        "report",
        "posted-content"
    ]

    def __init__(self, mailto, user_agent, logger_instance=None, log_candidates=False, candidate_log_file="crossref_candidates.log"):
        if not mailto or not user_agent:
            raise ValueError(
                "mailto and user_agent are required for Strategy initialization.")
        self.mailto = mailto
        self.user_agent = user_agent
        self.logger = logger_instance if logger_instance else logging.getLogger(
            __name__)
        if not logger_instance:
            logging.basicConfig(
                level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        self.log_candidates = log_candidates
        self.candidate_log_file = candidate_log_file
        if self.log_candidates:
            self.logger.info(f"Candidate logging enabled. Raw candidates will be saved to: {self.candidate_log_file}")

    def _normalize_string(self, text):
        if text is None:
            return ""
        try:
            text_str = str(text)
            normalized_text = unicodedata.normalize('NFC', text_str)
            normalized_text = unidecode(normalized_text)
            normalized_text = normalized_text.lower()
            normalized_text = re.sub(
                r'[\u2010\u2011\u2012\u2013\u2014\u2015]', '-', normalized_text)
            normalized_text = re.sub(
                r'[^\w\s-]', '', normalized_text, flags=re.UNICODE)
            normalized_text = re.sub(r'\s+', ' ', normalized_text).strip()
            return normalized_text
        except Exception as e:
            self.logger.error(f"Error normalizing string '{str(text)[:100]}...': {e}", exc_info=True)
            temp_text = re.sub(r'\s+', ' ', str(text)).strip().lower()
            temp_text = re.sub(r'[^\w\s-]', '', temp_text,
                               flags=re.UNICODE)  # Basic fallback
            return temp_text

    def match(self, input_json_string):
        try:
            article_datacite = json.loads(input_json_string)
            input_doi = article_datacite.get(
                "id") or article_datacite.get("doi", "N/A")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding input JSON: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error processing input JSON structure: {e}")
            return []

        candidates_crossref = self.get_candidates(
            article_datacite,
            self.mailto,
            self.user_agent
        )

        filtered_candidates = [
            r for r in candidates_crossref
            if isinstance(r, dict) and r.get("type", "").lower() in self.accepted_crossref_types
        ]
        if len(filtered_candidates) < len(candidates_crossref):
            self.logger.debug(f"Filtered {len(candidates_crossref) - len(filtered_candidates)} candidates by type for Input DOI {input_doi}.")

        matches = self.match_candidates(article_datacite, filtered_candidates)
        return matches

    def get_candidates(self, article_datacite, mailto, user_agent):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        query = self.candidate_query(article_datacite)
        if not query:
            self.logger.warning(f"No query generated for DOI {input_doi}")
            return []

        params = {
            "query.bibliographic": query,
            "rows": 25
        }

        code, results = crossref_rest_api_call(
            route="works",
            params=params,
            mailto=mailto,
            user_agent=user_agent
        )

        if self.log_candidates:
            try:
                log_entry = {
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "input_doi": input_doi,
                    "query": query,
                    "api_status_code": code,
                    "retrieved_candidates": []
                }
                if code == 200 and isinstance(results, dict) and "items" in results:
                    log_entry["retrieved_candidates"] = results.get(
                        "items", [])
                elif code != 200:
                    log_entry["error_details"] = str(results)[:500]

                with open(self.candidate_log_file, 'a', encoding='utf-8') as f:
                    json.dump(log_entry, f, ensure_ascii=False)
                    f.write('\n')
            except IOError as e:
                self.logger.error(f"Failed to write to candidate log file {self.candidate_log_file}: {e}", exc_info=True)
            except Exception as e:
                self.logger.error(f"An unexpected error occurred during candidate logging: {e}", exc_info=True)

        if code != 200 or not results:
            error_detail = ""
            try:
                if isinstance(results, requests.Response):
                    error_detail = results.text[:200]
                elif isinstance(results, dict):
                    error_detail = str(results.get(
                        "message", {}).get("error", results))[:200]
                else:
                    error_detail = str(results)[:200]
            except Exception:
                error_detail = "Could not extract error details."
            self.logger.error(f"Crossref API call failed for Input DOI {input_doi}. Status: {code}. Details: {error_detail}")
            return []

        if not isinstance(results, dict) or "items" not in results:
            self.logger.warning(f"Unexpected Crossref API response structure for Input DOI {input_doi}. Response: {str(results)[:200]}")
            return []

        items = results.get("items", [])
        if not isinstance(items, list):
            self.logger.warning(f"Crossref API 'items' is not a list for Input DOI {input_doi}")
            return []

        return items

    def match_candidates(self, article_datacite, candidates_crossref):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        if not candidates_crossref:
            self.logger.debug(f"No candidates to score for Input DOI {input_doi} after type filtering.")
            return []

        scores = []
        for cand in candidates_crossref:
            if isinstance(cand, dict):
                cand_doi = cand.get("DOI")
                if cand_doi:
                    score = self.score(article_datacite, cand)
                    if score is not None:
                        scores.append((cand_doi, score))
                else:
                    self.logger.debug(f"Candidate missing DOI for Input DOI {input_doi}: {str(cand)[:100]}")
            else:
                self.logger.debug(f"Candidate is not a dict for Input DOI {input_doi}: {str(cand)[:100]}")

        matches = [(d, s) for d, s in scores if s >= self.min_score]

        if not matches:
            self.logger.debug(f"No candidates met min_score ({self.min_score}) for Input DOI {input_doi}.")
            return []

        try:
            top_score = max(s for _, s in matches)
        except ValueError:
            self.logger.warning(f"Could not determine top score for DOI {input_doi}, though matches existed.")
            return []

        final_matches = [
            (d, s) for d, s in matches if top_score - s < self.max_score_diff
        ]

        formatted_results = [
            {
                "id": doi_id(doi),
                "confidence": round(score, 4),
                "strategies": [self.strategy],
            }
            for doi, score in final_matches
        ]
        self.logger.info(f"Found {len(formatted_results)} final match(es) for Input DOI {input_doi}.")
        return formatted_results

    def candidate_query(self, article_datacite):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        attributes = article_datacite.get("attributes", {})
        if not isinstance(attributes, dict):
            self.logger.warning(f"Input attributes missing or not a dict for DOI {input_doi}")
            return ""

        title = ""
        titles_list = attributes.get("titles", [])
        main_title = ""
        subtitle = ""
        if isinstance(titles_list, list):
            for t in titles_list:
                if isinstance(t, dict):
                    t_type = t.get("titleType", "").lower() if t.get(
                        "titleType") else "main"
                    t_title = t.get("title", "")
                    if t_title:
                        if t_type == "main" and not main_title:
                            main_title = t_title
                        elif t_type == "subtitle" and not subtitle:
                            subtitle = t_title
            if not main_title and titles_list and isinstance(titles_list[0], dict):
                main_title = titles_list[0].get("title", "")

        if main_title:
            title = main_title
            if subtitle:
                title = f"{main_title}: {subtitle}"

        if title:
            title = re.sub("&amp;", "&", title)
            title = re.sub("&lt;", "<", title)
            title = re.sub("&gt;", ">", title)
            title = re.sub(r'\s+', ' ', title).strip()

        author_names = []
        people = []
        creators = attributes.get("creators", [])
        contributors = attributes.get("contributors", [])
        if isinstance(creators, list):
            people.extend(creators)
        if isinstance(contributors, list):
            people.extend([p for p in contributors if isinstance(
                p, dict) and p.get("nameType") == "Personal"])

        for person in people:
            if isinstance(person, dict) and person.get("nameType") == "Personal":
                family_name = person.get("familyName")
                if family_name:
                    author_names.append(family_name.strip())

        authors_str = " ".join(sorted(list(set(filter(None, author_names)))))

        year_str = ""
        pub_year = attributes.get("publicationYear")
        if pub_year:
            try:
                year_str = str(int(pub_year))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid publicationYear '{pub_year}' for DOI {input_doi}")
                year_str = ""

        query_parts = [part for part in [title, authors_str, year_str] if part]
        full_query = " ".join(query_parts)
        full_query = re.sub(r'\s+', ' ', full_query).strip()

        if not full_query:
            self.logger.warning(f"Empty query generated for DOI {input_doi}")
            return ""

        final_query = full_query[:self.max_query_len]
        if len(full_query) > self.max_query_len:
            self.logger.warning(f"Query truncated for DOI {input_doi}. Original length: {len(full_query)}")

        self.logger.debug(f"Generated Query for {input_doi}: '{final_query[:200]}...'")
        return final_query

    def score(self, article_datacite, preprint_crossref):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        preprint_doi = preprint_crossref.get(
            "DOI", "N/A") if isinstance(preprint_crossref, dict) else "N/A"

        if not isinstance(preprint_crossref, dict):
            self.logger.warning(f"Candidate {preprint_doi} is not a dictionary during scoring.")
            return None

        y_score = self.year_score(article_datacite, preprint_crossref)
        t_score = self.title_score(article_datacite, preprint_crossref)
        a_score = self.authors_score(article_datacite, preprint_crossref)

        if y_score is None or t_score is None or a_score is None:
            self.logger.warning(f"Failed to calculate one or more score components for Input:{input_doi} vs Cand:{preprint_doi}")
            return None

        weighted_sum = (self.weight_year * y_score +
                        self.weight_title * t_score +
                        self.weight_author * a_score)

        total_weight = (self.weight_year +
                        self.weight_title +
                        self.weight_author)

        if total_weight == 0:
            final_score = 0.0
        else:
            final_score = weighted_sum / total_weight

        self.logger.debug(f"Scores for Input:{input_doi} vs Cand:{preprint_doi}: Year={y_score:.3f}, Title={t_score:.3f}, Author={a_score:.3f} -> Final={final_score:.3f}")
        return final_score

    def year_score(self, article_datacite, preprint_crossref):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        preprint_doi = preprint_crossref.get("DOI", "N/A")

        try:
            article_year = 0
            attributes = article_datacite.get("attributes", {})
            if isinstance(attributes, dict):
                pub_year = attributes.get("publicationYear")
                if pub_year:
                    try:
                        article_year = int(pub_year)
                    except (ValueError, TypeError):
                        self.logger.debug(f"Invalid publicationYear '{pub_year}' in DataCite for {input_doi}")

            preprint_year = 0
            date_sources = [
                preprint_crossref.get("published-online"),
                preprint_crossref.get("published-print"),
                preprint_crossref.get("issued"),
                preprint_crossref.get("created"),
            ]

            for date_info in date_sources:
                if isinstance(date_info, dict) and date_info.get("date-parts"):
                    date_parts = date_info["date-parts"]
                    if isinstance(date_parts, list) and date_parts and \
                       isinstance(date_parts[0], list) and date_parts[0]:
                        year_part = date_parts[0][0]
                        if year_part is not None:
                            try:
                                potential_year = int(year_part)
                                if 1800 < potential_year < 2100:
                                    preprint_year = potential_year
                                    break
                            except (ValueError, TypeError):
                                continue
            if not preprint_year:
                self.logger.debug(f"Could not extract valid publication year for Cand:{preprint_doi}")

            if not article_year or not preprint_year:
                self.logger.debug(f"Missing year for comparison: InputYear={article_year}, CandYear={preprint_year}. Input:{input_doi}, Cand:{preprint_doi}")
                return 0.0

            year_diff = preprint_year - article_year

            if year_diff < 0:
                score = 0.0
            elif 0 <= year_diff <= 2:
                score = 1.0
            elif year_diff == 3:
                score = 0.9
            elif year_diff == 4:
                score = 0.8
            else:
                score = 0.0

            return score

        except (ValueError, TypeError, IndexError, AttributeError) as e:
            self.logger.error(f"Error calculating year score for Input:{input_doi} vs Cand:{preprint_doi}: {e}", exc_info=True)
            return None

    def title_score(self, article_datacite, preprint_crossref):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        preprint_doi = preprint_crossref.get("DOI", "N/A")

        try:
            article_title_raw = ""
            attributes = article_datacite.get("attributes", {})
            if isinstance(attributes, dict):
                titles_list = attributes.get("titles", [])
                main_title = ""
                subtitle = ""
                if isinstance(titles_list, list):
                    for t in titles_list:
                        if isinstance(t, dict):
                            t_type = t.get("titleType", "").lower() if t.get(
                                "titleType") else "main"
                            t_title = t.get("title", "")
                            if t_title:
                                if t_type == "main" and not main_title:
                                    main_title = t_title
                                elif t_type == "subtitle" and not subtitle:
                                    subtitle = t_title
                    if not main_title and titles_list and isinstance(titles_list[0], dict):
                        main_title = titles_list[0].get("title", "")
                    if main_title:
                        article_title_raw = main_title
                        if subtitle:
                            article_title_raw = f"{main_title}: {subtitle}"

            preprint_title_raw = ""
            crossref_titles = preprint_crossref.get("title", [])
            if isinstance(crossref_titles, list) and crossref_titles:
                first_cr_title = crossref_titles[0]
                if isinstance(first_cr_title, str):
                    preprint_title_raw = first_cr_title
            crossref_subtitles = preprint_crossref.get("subtitle", [])
            if preprint_title_raw and isinstance(crossref_subtitles, list) and crossref_subtitles:
                first_cr_subtitle = crossref_subtitles[0]
                if isinstance(first_cr_subtitle, str):
                    preprint_title_raw = f"{preprint_title_raw}: {first_cr_subtitle}"

            article_title_norm = self._normalize_string(article_title_raw)
            preprint_title_norm = self._normalize_string(preprint_title_raw)

            if not article_title_norm or not preprint_title_norm:
                self.logger.debug(f"Missing normalized title for comparison: Input:'{article_title_norm}' vs Cand:'{preprint_title_norm}' (Input DOI:{input_doi}, Cand DOI:{preprint_doi})")
                return 0.0

            score_ts = fuzz.token_set_ratio(
                article_title_norm, preprint_title_norm) / 100.0
            score_tso = fuzz.token_sort_ratio(
                article_title_norm, preprint_title_norm) / 100.0
            score_r = fuzz.ratio(article_title_norm,
                                 preprint_title_norm) / 100.0
            score = (score_ts * 0.45 + score_tso * 0.45 + score_r * 0.10)

            def differ_by_keywords(title1_norm, title2_norm):
                words1 = title1_norm.split()[:3]
                words2 = title2_norm.split()[:3]
                keywords = {self._normalize_string(k) for k in [
                    "correction", "response", "reply", "appendix", "erratum", "corrigendum", "comment", "addendum"
                ]}
                t1_has_keyword = any(word in keywords for word in words1)
                t2_has_keyword = any(word in keywords for word in words2)
                differs = t1_has_keyword != t2_has_keyword
                return differs

            if differ_by_keywords(article_title_norm, preprint_title_norm):
                self.logger.debug(f"Applying title keyword penalty for Input:{input_doi} vs Cand:{preprint_doi}")
                score *= 0.67

            return score

        except Exception as e:
            self.logger.error(f"Error calculating title score for Input:{input_doi} vs Cand:{preprint_doi}: {e}", exc_info=True)
            return None

    def authors_score(self, article_datacite, preprint_crossref):
        input_doi = article_datacite.get(
            "id") or article_datacite.get("doi", "N/A")
        preprint_doi = preprint_crossref.get("DOI", "N/A")

        try:
            attributes = article_datacite.get("attributes", {})
            if not isinstance(attributes, dict):
                self.logger.warning(f"Missing attributes for author extraction in Input:{input_doi}")
                return 0.0

            datacite_people = []
            creators = attributes.get("creators", [])
            contributors = attributes.get("contributors", [])
            if isinstance(creators, list):
                datacite_people.extend(creators)
            if isinstance(contributors, list):
                datacite_people.extend([p for p in contributors if isinstance(
                    p, dict) and p.get("nameType") == "Personal"])

            crossref_author_list = preprint_crossref.get("author", [])
            if not isinstance(crossref_author_list, list):
                self.logger.warning(f"Crossref 'author' field not a list for Cand:{preprint_doi}")
                crossref_author_list = []

            article_authors_norm = self._normalize_authors(
                datacite_people, 'datacite', input_doi)
            preprint_authors_norm = self._normalize_authors(
                crossref_author_list, 'crossref', preprint_doi)

            if article_authors_norm is None or preprint_authors_norm is None:
                self.logger.error(f"Author normalization failed for Input:{input_doi} vs Cand:{preprint_doi}")
                return None

            len1 = len(article_authors_norm)
            len2 = len(preprint_authors_norm)

            if len1 == 0 and len2 == 0:
                return 0.5
            if len1 == 0 or len2 == 0:
                penalty_score = 0.1 if max(len1, len2) < 3 else 0.0
                self.logger.debug(f"One author list empty for Input:{input_doi} vs Cand:{preprint_doi}. Score: {penalty_score}")
                return penalty_score

            total_authors = len1 + len2

            author_list_heuristic_threshold = 50
            if total_authors > author_list_heuristic_threshold:
                self.logger.debug(f"Using family name heuristic for large author list ({total_authors}) for Input:{input_doi} vs Cand:{preprint_doi}")
                article_families_norm = " ".join(
                    sorted([a['family'] for a in article_authors_norm if a.get('family')]))
                preprint_families_norm = " ".join(
                    sorted([a['family'] for a in preprint_authors_norm if a.get('family')]))

                if not article_families_norm or not preprint_families_norm:
                    return 0.0
                score = fuzz.token_sort_ratio(
                    article_families_norm, preprint_families_norm) / 100.0
                return score

            score_sum = 0.0
            remaining_article_authors = article_authors_norm[:]
            remaining_preprint_authors = preprint_authors_norm[:]

            match_count = 0
            while remaining_article_authors and remaining_preprint_authors:
                best_score, idx1, idx2 = self._find_most_similar_author_pair(
                    remaining_article_authors, remaining_preprint_authors
                )

                if best_score < 0:
                    break
                similarity_threshold_for_match = 0.5
                if best_score < similarity_threshold_for_match:
                    break

                score_sum += best_score
                match_count += 1

                if 0 <= idx1 < len(remaining_article_authors) and 0 <= idx2 < len(remaining_preprint_authors):
                    if idx1 >= idx2:
                        del remaining_article_authors[idx1]
                        del remaining_preprint_authors[idx2]
                    else:
                        del remaining_preprint_authors[idx2]
                        del remaining_article_authors[idx1]
                else:
                    self.logger.error(f"Invalid indices ({idx1}, {idx2}) during author matching for Input:{input_doi} vs Cand:{preprint_doi}")
                    break

            if total_authors > 0:
                final_score = (2.0 * score_sum) / total_authors
            else:
                final_score = 0.5

            final_score = max(0.0, min(1.0, final_score))
            return final_score

        except Exception as e:
            self.logger.error(f"Error calculating authors score for Input:{input_doi} vs Cand:{preprint_doi}: {e}", exc_info=True)
            return None

    def _normalize_authors(self, authors_list, source_format, context_doi="N/A"):
        normalized = []
        if not isinstance(authors_list, list):
            self.logger.warning(f"Author list is not a list in {source_format} data for DOI {context_doi}.")
            return []

        try:
            for i, person in enumerate(authors_list):
                if not isinstance(person, dict):
                    self.logger.debug(f"Skipping non-dict author item {i} in {source_format} for DOI {context_doi}")
                    continue

                if source_format == 'datacite' and person.get("nameType") == "Organizational":
                    self.logger.debug(f"Skipping Organizational author/contributor in {source_format} for DOI {context_doi}: {person.get('name')}")
                    continue
                elif source_format == 'crossref' and (person.get('family') is None and person.get('name') is not None):
                    # Crossref works can have organizational authors without explicit type
                    # Heuristic: if no given name, likely org
                    if not person.get('given'):
                        self.logger.debug(f"Skipping likely Organizational author in {source_format} for DOI {context_doi}: {person.get('name')}")
                        continue

                raw_given = ''
                raw_family = ''
                raw_name_field = ''

                if source_format == 'datacite':
                    raw_given = person.get('givenName', '') or ''
                    raw_family = person.get('familyName', '') or ''
                    raw_name_field = person.get('name', '') or ''

                    if raw_name_field and not raw_family and not raw_given:
                        if ',' in raw_name_field:
                            parts = raw_name_field.split(',', 1)
                            raw_family = parts[0].strip()
                            raw_given = parts[1].strip()
                        else:
                            parts = raw_name_field.split()
                            if len(parts) > 1:
                                raw_family = parts[-1]
                                raw_given = " ".join(parts[:-1])
                            # else: keep empty, handled later

                elif source_format == 'crossref':
                    raw_given = person.get('given', '') or ''
                    raw_family = person.get('family', '') or ''
                    raw_name_field = person.get('name', '') or ''

                norm_given = self._normalize_string(raw_given)
                norm_family = self._normalize_string(raw_family)
                norm_name_field = self._normalize_string(raw_name_field)

                final_given = norm_given
                final_family = norm_family

                if not final_family and norm_name_field:
                    if ',' in norm_name_field:
                        parts = norm_name_field.split(',', 1)
                        final_family = parts[0].strip()
                        if not final_given:
                            final_given = parts[1].strip()
                    else:
                        parts = norm_name_field.split()
                        if len(parts) > 1:
                            final_family = parts[-1]
                            if not final_given:
                                final_given = " ".join(parts[:-1])
                        elif len(parts) == 1 and not final_given:
                            # Only one name part, assume it's family if no other info
                            final_family = parts[0]

                initials = "".join(part[0]
                                   for part in re.findall(r'\b\w', final_given))

                orcid_val = ''
                if source_format == 'datacite':
                    name_identifiers = person.get('nameIdentifiers', [])
                    if isinstance(name_identifiers, list):
                        for identifier in name_identifiers:
                            if isinstance(identifier, dict):
                                scheme = (identifier.get(
                                    'nameIdentifierScheme', '') or '').upper()
                                uri = (identifier.get(
                                    'schemeUri', '') or '').lower()
                                if scheme == 'ORCID' or 'orcid.org' in uri:
                                    orcid_val = identifier.get(
                                        'nameIdentifier', '') or ''
                                    if orcid_val:
                                        break
                elif source_format == 'crossref':
                    orcid_val = person.get('ORCID', '') or ''

                norm_orcid = ''
                if orcid_val:
                    cleaned_orcid = re.sub(
                        r'^https?://orcid.org/', '', str(orcid_val).strip())
                    cleaned_orcid = re.sub(
                        r"[^0-9X]", "", cleaned_orcid.upper())
                    if re.match(r"^\d{15}[\dX]$", cleaned_orcid):
                        norm_orcid = cleaned_orcid
                    else:
                        self.logger.debug(f"Ignoring invalid ORCID format for DOI {context_doi}: {orcid_val} (cleaned: {cleaned_orcid})")

                if final_family or final_given or norm_orcid:
                    author_entry = {
                        'given': final_given,
                        'family': final_family,
                        'initials': initials,
                        'orcid': norm_orcid
                    }
                    author_entry['name_variations'] = self._get_author_name_variations(
                        author_entry)
                    normalized.append(author_entry)
                else:
                    self.logger.debug(f"Skipping author with insufficient info in {source_format} for DOI {context_doi}: {str(person)[:100]}")

            return normalized
        except Exception as e:
            self.logger.error(f"Error during author normalization (format: {source_format}, DOI: {context_doi}): {e}", exc_info=True)
            return None

    def _find_most_similar_author_pair(self, authors1_norm, authors2_norm):
        if not authors1_norm or not authors2_norm:
            return -1.0, -1, -1

        best_score = -1.0
        best_i1 = -1
        best_i2 = -1

        for i1, a1 in enumerate(authors1_norm):
            for i2, a2 in enumerate(authors2_norm):
                try:
                    score = self._score_normalized_author_similarity(a1, a2)
                    if score is None:
                        continue

                    if score > best_score:
                        best_score = score
                        best_i1 = i1
                        best_i2 = i2

                    if best_score >= 0.999:
                        break
                except Exception as e:
                    self.logger.error(f"Error comparing author pair ({str(a1)[:50]}, {str(a2)[:50]}): {e}", exc_info=True)
                    continue

            if best_score >= 0.999:
                break

        final_best_score = max(0.0, best_score) if best_score > -1.0 else -1.0
        return final_best_score, best_i1, best_i2

    def _score_normalized_author_similarity(self, author1_norm, author2_norm):
        try:
            orcid1 = author1_norm.get('orcid')
            orcid2 = author2_norm.get('orcid')
            if orcid1 and orcid2:
                return 1.0 if orcid1 == orcid2 else 0.0

            names1 = author1_norm.get('name_variations')
            names2 = author2_norm.get('name_variations')

            if names1 is None or names2 is None:
                self.logger.warning(
                    "Failed to get pre-computed name variations for similarity scoring.")
                return None

            if not names1 or not names2:
                f1 = author1_norm.get('family')
                f2 = author2_norm.get('family')
                if f1 and f2 and f1 == f2:
                    return 0.3
                return 0.0

            best_name_score = 0.0
            for n1 in names1:
                current_best_for_n1 = 0.0
                for n2 in names2:
                    score = fuzz.token_sort_ratio(n1, n2) / 100.0
                    if score > current_best_for_n1:
                        current_best_for_n1 = score
                    if current_best_for_n1 > 0.99:
                        break
                if current_best_for_n1 > best_name_score:
                    best_name_score = current_best_for_n1
                if best_name_score > 0.99:
                    break

            f1 = author1_norm.get('family')
            f2 = author2_norm.get('family')
            if f1 and f2 and f1 == f2 and best_name_score > 0.6:
                best_name_score = min(1.0, best_name_score * 1.1)

            return best_name_score

        except Exception as e:
            self.logger.error(f"Error scoring normalized author similarity between {str(author1_norm)[:50]} and {str(author2_norm)[:50]}: {e}", exc_info=True)
            return None

    def _get_author_name_variations(self, author_norm):
        try:
            names = set()
            g = author_norm.get('given', '')
            f = author_norm.get('family', '')
            initials = author_norm.get('initials', '')
            initials_nospace = re.sub(r'\s', '', initials)

            if f and len(f) > 1:
                names.add(f)

            if g and f:
                full_gf = f"{g} {f}"
                full_fg = f"{f} {g}"
                names.add(full_gf)
                names.add(full_fg)

            if initials_nospace and f:
                initials_space = " ".join(initials_nospace)
                names.add(f"{initials_nospace} {f}")
                names.add(f"{initials_space} {f}")
                names.add(f"{f} {initials_nospace}")
                names.add(f"{f} {initials_space}")

                if initials_nospace:
                    first_initial = initials_nospace[0]
                    names.add(f"{first_initial} {f}")
                    names.add(f"{f} {first_initial}")

            if g and len(g) > 1 and g != f:
                names.add(g)

            if initials_nospace and len(initials_nospace) > 1:
                names.add(initials_nospace)
                names.add(" ".join(initials_nospace))

            final_names = {self._normalize_string(name) for name in names}
            final_names = {name for name in final_names if len(name) > 1}

            if not final_names:
                if f and len(f) == 1 and not g:
                    final_names.add(f)
                if g and len(g) == 1 and not f:
                    final_names.add(g)

            return final_names if final_names else set()

        except Exception as e:
            self.logger.error(f"Error getting author name variations for {str(author_norm)[:50]}: {e}", exc_info=True)
            return None
