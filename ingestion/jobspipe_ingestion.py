
from pathlib import Path
import json

from ingestion.common import configure_logging, ingest_csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "jobspipe"
    / "jobspipe_jobs.csv"
)

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "jobspipe"

EXPECTED_COLUMNS = [
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

logger = configure_logging(PROJECT_ROOT)


def ingest_jobspipe() -> dict:
    """Ingest the JobsPipe collection into Bronze."""

    return ingest_csv(
        source_name="jobspipe",
        source_path=SOURCE_PATH,
        bronze_root=BRONZE_ROOT,
        expected_columns=EXPECTED_COLUMNS,
        bronze_file_name="jobspipe_jobs.csv",
        logger=logger,
    )


if __name__ == "__main__":
    result = ingest_jobspipe()
    print("\nJobsPipe ingestion result:")
    print(json.dumps(result, indent=2))
