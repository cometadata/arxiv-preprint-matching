import json
import requests
from datetime import timedelta
from ratelimit import limits, sleep_and_retry
from starlette.responses import JSONResponse

CROSSREF_API_BASE_URL = "https://api.crossref.org"
DEFAULT_RATE_LIMIT_CALLS = 10
DEFAULT_RATE_LIMIT_PERIOD = 1


@sleep_and_retry
@limits(calls=DEFAULT_RATE_LIMIT_CALLS, period=timedelta(seconds=DEFAULT_RATE_LIMIT_PERIOD).total_seconds())
def crossref_rest_api_call(route, params, mailto, user_agent):
    url = f"{CROSSREF_API_BASE_URL}/{route.lstrip('/')}"
    headers = {
        'User-Agent': user_agent
    }
    params["mailto"] = mailto

    try:
        response = requests.get(
            url, params=params, headers=headers, timeout=30)
        code = response.status_code

        if code == 200:
            try:
                result = response.json()
                return code, result.get("message", result)
            except json.JSONDecodeError:
                print(f"Warning: Received non-JSON response for route {route} with status 200.")
                return code, response.text
        else:
            print(f"Warning: Crossref API call to {route} failed with status code {code}.")
            return code, response

    except requests.exceptions.RequestException as e:
        print(f"Error during Crossref API call to {route}: {e}")
        return 503, {"error": str(e)}


def doi_id(doi_str):
    if doi_str is None:
        return None
    doi_str = doi_str.lower().replace("doi:", "").strip()
    return f"https://doi.org/{doi_str}"


class AsciiJSONResponse(JSONResponse):
    def render(self, content):
        return json.dumps(content, ensure_ascii=True).encode("utf-8")
