
from pathlib import Path
import json

from ingestion.common import configure_logging, ingest_csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "rozee"
    / "RozeePK_Jobs.csv"
)

BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "rozee"

EXPECTED_COLUMNS = [
    "Title",
    "Salary",
    "Job Type",
    "Job Location",
    "Functional Area",
    "Career Level",
    "Apply Before",
    "Minimum Experience",
    "Minimum Education",
    "Gender",
    "Age",
    "Skills",
]

logger = configure_logging(PROJECT_ROOT)


def ingest_rozee() -> dict:
    return ingest_csv(
        source_name="rozee",
        source_path=SOURCE_PATH,
        bronze_root=BRONZE_ROOT,
        expected_columns=EXPECTED_COLUMNS,
        bronze_file_name="RozeePK_Jobs.csv",
        logger=logger,
    )


if __name__ == "__main__":
    result = ingest_rozee()
    print("\nIngestion result:")
    print(json.dumps(result, indent=2))
