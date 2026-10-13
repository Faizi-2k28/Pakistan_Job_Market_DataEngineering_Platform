import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API_URL = "https://api.jobspipe.dev/v1/jobs/search"


def main() -> int:
    api_key = os.environ.get("JOBSPIPE_API_KEY")

    if not api_key:
        print("API key is missing from the environment.")
        return 1

    payload = {
        "job_country_code_or": ["PK"],
        "limit": 5,
        "include_total_results": True,
    }

    request = Request(
        API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=30) as response:
            result = json.loads(response.read().decode("utf-8"))

    except HTTPError as exc:
        print(f"API request failed with HTTP {exc.code}.")
        if exc.code == 401:
            print("Check the new API key and its permissions.")
        elif exc.code == 402:
            print("Check the account quota or billing status.")
        elif exc.code == 429:
            print("Rate limit reached. Wait before retrying.")
        return 1

    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"Could not complete the API request: {exc}")
        return 1

    jobs = result.get("data", [])
    metadata = result.get("metadata", {})

    print("API request succeeded.")
    print("Pakistan listings returned:", len(jobs))
    print("Reported total results:", metadata.get("total_results"))
    print("Credits remaining:", metadata.get("credits_remaining"))

    for index, job in enumerate(jobs, start=1):
        print(f"\nJob {index}")
        print("Title:", job.get("job_title") or "Unknown")
        print("Company:", job.get("company") or "Unknown")
        print("Location:", job.get("location") or "Unknown")
        print("Country:", job.get("country_code") or "Unknown")
        print("Job URL:", job.get("final_url") or job.get("url") or "")

    return 0


if __name__ == "__main__":
    sys.exit(main())
