import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ROOT = Path("/opt/project")

REQUIRED_FILES = [
    "data/source/rozee/RozeePK_Jobs.csv",
    "data/source/pakistan_jobs/pakistan_jobs.csv",
    "data/source/jobspipe/jobspipe_jobs.csv",
    "collectors/collect_jobspipe.py",
    "ingestion/run_ingestion.py",
    "spark_jobs/build_silver.py",
    "spark_jobs/build_gold.py",
    "spark_jobs/export_gold_for_powerbi.py",
]


# ============================================================
# COMMON SCRIPT EXECUTION FUNCTION
# ============================================================

def execute_project_script(script_relative_path: str) -> None:
    """
    Execute a project script inside the Airflow Linux container.
    Stop the task if the script fails.
    """

    script_path = PROJECT_ROOT / script_relative_path

    if not script_path.is_file():
        raise FileNotFoundError(
            f"Pipeline script not found: {script_path}"
        )

    environment = os.environ.copy()

    # Make the project modules available to Python.
    existing_pythonpath = environment.get("PYTHONPATH", "")

    environment["PYTHONPATH"] = (
        str(PROJECT_ROOT)
        + os.pathsep
        + existing_pythonpath
    )

    print(f"Starting script: {script_relative_path}")

    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(PROJECT_ROOT),
        env=environment,
        check=True,
        text=True,
    )

    print(f"Script completed successfully: {script_relative_path}")


# ============================================================
# AIRFLOW DAG DEFINITION
# ============================================================

@dag(
    dag_id="pakistan_job_market_data_pipeline",
    description=(
        "Collect JobsPipe listings, ingest job sources, "
        "build Silver and Gold datasets, and export Power BI data."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["job_market", "spark", "portfolio"],
)
def pakistan_job_market_data_pipeline():

    # --------------------------------------------------------
    # TASK 1: VALIDATE PROJECT INPUTS
    # --------------------------------------------------------

    @task
    def validate_project_inputs() -> None:
        """Validate files, scripts, and API configuration."""

        if not PROJECT_ROOT.is_dir():
            raise FileNotFoundError(
                f"Project directory not found: {PROJECT_ROOT}"
            )

        missing_files = [
            relative_path
            for relative_path in REQUIRED_FILES
            if not (PROJECT_ROOT / relative_path).is_file()
        ]

        if missing_files:
            raise FileNotFoundError(
                "Missing required project files: "
                + ", ".join(missing_files)
            )

        api_key = os.environ.get("JOBSPIPE_API_KEY", "").strip()

        if not api_key:
            raise EnvironmentError(
                "JOBSPIPE_API_KEY is missing from the Airflow environment."
            )

        print("All required project files are available.")
        print("JobsPipe API key is configured.")

    # --------------------------------------------------------
    # TASK 2: COLLECT JOBSPIPE DATA
    # --------------------------------------------------------

    @task
    def collect_jobspipe() -> None:
        """
        Fetch one page of JobsPipe listings and update
        the cumulative source CSV.
        """

        execute_project_script(
            "collectors/collect_jobspipe.py"
        )

    # --------------------------------------------------------
    # TASK 3: INGEST ALL SOURCE DATA
    # --------------------------------------------------------

    @task
    def ingest_sources() -> None:
        """Ingest Rozee, Pakistan Jobs, and JobsPipe data."""

        execute_project_script(
            "ingestion/run_ingestion.py"
        )

    # --------------------------------------------------------
    # TASK 4: BUILD SILVER LAYER
    # --------------------------------------------------------

    @task
    def build_silver() -> None:
        """Clean, standardize, and deduplicate job records."""

        execute_project_script(
            "spark_jobs/build_silver.py"
        )

    # --------------------------------------------------------
    # TASK 5: BUILD GOLD LAYER
    # --------------------------------------------------------

    @task
    def build_gold() -> None:
        """Generate analytical datasets for reporting."""

        execute_project_script(
            "spark_jobs/build_gold.py"
        )

    # --------------------------------------------------------
    # TASK 6: EXPORT DATA FOR POWER BI
    # --------------------------------------------------------

    @task
    def export_powerbi_data() -> None:
        """Export Gold datasets as verified Power BI CSV files."""

        execute_project_script(
            "spark_jobs/export_gold_for_powerbi.py"
        )

    # --------------------------------------------------------
    # DEFINE TASK EXECUTION ORDER
    # --------------------------------------------------------

    validation = validate_project_inputs()

    collection = collect_jobspipe()

    ingestion = ingest_sources()

    silver = build_silver()

    gold = build_gold()

    export = export_powerbi_data()

    validation >> collection >> ingestion >> silver >> gold >> export


# Register the DAG with Airflow.
pakistan_job_market_data_pipeline()