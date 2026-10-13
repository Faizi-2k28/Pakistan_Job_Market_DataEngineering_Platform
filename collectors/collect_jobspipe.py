
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "source" / "jobspipe"
OUTPUT_FILE = OUTPUT_DIR / "jobspipe_jobs.csv"

API_URL = "https://api.jobspipe.dev/v1/jobs/search"
PAGE_SIZE = 25

CSV_COLUMNS = [
    "jobspipe_id",
    "job_title",
    "company_name",
    "location",
    "country",
    "country_code",
    "date_posted",
    "discovered_at",
    "salary_string",
    "salary_currency",
    "min_annual_salary",
    "max_annual_salary",
    "employment_type",
    "seniority",
    "skills",
    "upstream_source",
    "job_url",
    "collected_at",
]


def list_to_text(value) -> str:
    """Convert API lists into a consistent semicolon-separated string."""
    if isinstance(value, list):
        return "; ".join(str(item) for item in value if item is not None)
    if value is None:
        return ""
    return str(value)


def normalize_job(job: dict, collected_at: str) -> dict:
    """Keep structured job fields needed for market analysis."""

    salary_min = job.get("min_annual_salary")
    salary_max = job.get("max_annual_salary")

    # Keep only explicitly supplied compensation values.
    # These API fields are annualized values, not monthly salaries.
    return {
        "jobspipe_id": str(job.get("id") or "").strip(),
        "job_title": job.get("job_title") or "",
        "company_name": job.get("company") or "",
        "location": job.get("location") or "",
        "country": job.get("country") or "",
        "country_code": job.get("country_code") or "",
        "date_posted": job.get("date_posted") or "",
        "discovered_at": job.get("discovered_at") or "",
        "salary_string": job.get("salary_string") or "",
        "salary_currency": job.get("salary_currency") or "",
        "min_annual_salary": (
            "" if salary_min is None else salary_min
        ),
        "max_annual_salary": (
            "" if salary_max is None else salary_max
        ),
        "employment_type": list_to_text(
            job.get("employment_statuses")
        ),
        "seniority": job.get("seniority") or "",
        "skills": list_to_text(job.get("keyword_slugs")),
        "upstream_source": job.get("source") or "",
        "job_url": (
            job.get("final_url")
            or job.get("url")
            or job.get("source_url")
            or ""
        ),
        "collected_at": collected_at,
    }


def load_existing_jobs() -> dict[str, dict]:
    """Load the current cumulative snapshot, keyed by stable API job ID."""

    if not OUTPUT_FILE.exists():
        return {}

    records = {}

    with OUTPUT_FILE.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            job_id = (row.get("jobspipe_id") or "").strip()

            if job_id:
                records[job_id] = {
                    column: row.get(column, "") or ""
                    for column in CSV_COLUMNS
                }

    return records


def fetch_one_page(api_key: str) -> tuple[list[dict], dict]:
    """Request at most one page of Pakistan job listings."""

    payload = {
        "job_country_code_or": ["PK"],
        "limit": PAGE_SIZE,
        "include_total_results": False,
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
        with urlopen(request, timeout=40) as response:
            result = json.loads(
                response.read().decode("utf-8")
            )

    except HTTPError as exc:
        if exc.code == 401:
            raise RuntimeError(
                "JobsPipe rejected the API key. Check its validity."
            ) from exc

        if exc.code == 402:
            raise RuntimeError(
                "JobsPipe reports that the account has no remaining credits."
            ) from exc

        if exc.code == 429:
            raise RuntimeError(
                "JobsPipe rate limit reached. Retry later."
            ) from exc

        raise RuntimeError(
            f"JobsPipe returned HTTP {exc.code}."
        ) from exc

    except URLError as exc:
        raise RuntimeError(
            f"Could not connect to JobsPipe: {exc.reason}"
        ) from exc

    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "JobsPipe returned invalid JSON."
        ) from exc

    jobs = result.get("data")

    if not isinstance(jobs, list):
        raise RuntimeError(
            "Unexpected API response: 'data' is not a list."
        )

    metadata = result.get("metadata") or {}

    return jobs, metadata


def main() -> int:
    api_key = os.environ.get("JOBSPIPE_API_KEY")

    if not api_key:
        print("Missing JOBSPIPE_API_KEY environment variable.")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    collected_at = datetime.now(timezone.utc).isoformat()

    try:
        jobs, metadata = fetch_one_page(api_key)

        existing_jobs = load_existing_jobs()
        previous_count = len(existing_jobs)

        for job in jobs:
            normalized = normalize_job(job, collected_at)
            job_id = normalized["jobspipe_id"]

            if not job_id:
                raise RuntimeError(
                    "An API record has no stable job ID. "
                    "The cumulative file was not updated."
                )

            # Upsert: update a known job or add a new one.
            existing_jobs[job_id] = normalized

        # Write to a temporary file first to avoid leaving a
        # partially written cumulative CSV if writing fails.
        temp_file = OUTPUT_FILE.with_suffix(".tmp")

        try:
            with temp_file.open(
                "w", newline="", encoding="utf-8"
            ) as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=CSV_COLUMNS,
                    extrasaction="ignore",
                )
                writer.writeheader()

                for job_id in sorted(existing_jobs):
                    writer.writerow(existing_jobs[job_id])

            temp_file.replace(OUTPUT_FILE)

        finally:
            if temp_file.exists():
                temp_file.unlink()

        new_count = len(existing_jobs) - previous_count
        updated_count = len(jobs)

        print("JobsPipe collection completed.")
        print(f"API records received: {len(jobs)}")
        print(f"New unique jobs added: {new_count}")
        print(f"Existing dataset rows: {len(existing_jobs)}")
        print(f"Records processed this run: {updated_count}")
        print(
            "Next API page available: "
            f"{bool(metadata.get('next_cursor'))}"
        )
        print(f"Output file: {OUTPUT_FILE}")

        if metadata.get("next_cursor"):
            print(
                "More API pages exist. This collector intentionally "
                "does not fetch them in the same run."
            )

        return 0

    except (RuntimeError, OSError, csv.Error) as exc:
        print(f"Collection failed: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
