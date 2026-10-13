
from pathlib import Path
import json

from ingestion.common import configure_logging, ingest_csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "pakistan_jobs"
    / "pakistan_jobs.csv"
)

BRONZE_ROOT = (
    PROJECT_ROOT / "data" / "bronze" / "pakistan_jobs"
)

EXPECTED_COLUMNS = [
    "jobTitle",
    "companyName",
    "postingDate",
    "salaryRange",
    "industry",
    "recruiterPhone",
    "location",
    "jobType",
    "skills",
    "recruiterEmail",
    "url",
]

logger = configure_logging(PROJECT_ROOT)


def ingest_pakistan_jobs() -> dict:
    return ingest_csv(
        source_name="pakistan_jobs",
        source_path=SOURCE_PATH,
        bronze_root=BRONZE_ROOT,
        expected_columns=EXPECTED_COLUMNS,
        bronze_file_name="pakistan_jobs.csv",
        logger=logger,
    )


if __name__ == "__main__":
    result = ingest_pakistan_jobs()
    print("\nIngestion result:")
    print(json.dumps(result, indent=2))
