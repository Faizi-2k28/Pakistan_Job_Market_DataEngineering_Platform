from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


PROJECT_ROOT = Path("/opt/project")

REQUIRED_FILES = [
    "ingestion/run_ingestion.py",
    "spark_jobs/build_silver.py",
    "spark_jobs/build_gold.py",
    "spark_jobs/export_gold_for_powerbi.py",
]


@dag(
    dag_id="job_market_pipeline_check",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["job_market", "setup"],
)
def job_market_pipeline_check():

    @task
    def verify_project_files() -> list[str]:
        if not PROJECT_ROOT.is_dir():
            raise FileNotFoundError(
                f"Project directory not found: {PROJECT_ROOT}"
            )

        missing_files = [
            file_name
            for file_name in REQUIRED_FILES
            if not (PROJECT_ROOT / file_name).is_file()
        ]

        if missing_files:
            raise FileNotFoundError(
                f"Required project files are missing: {missing_files}"
            )

        print("All required project files were found.")

        for file_name in REQUIRED_FILES:
            print(f"Verified: {file_name}")

        return REQUIRED_FILES

    @task
    def confirm_project_readiness(files: list[str]) -> None:
        print(f"Verified {len(files)} project files.")
        print("Project mount check completed successfully.")
        print("Spark runtime setup is the next step.")

    verified_files = verify_project_files()
    confirm_project_readiness(verified_files)


job_market_pipeline_check()
