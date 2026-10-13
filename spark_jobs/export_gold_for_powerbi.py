
from pathlib import Path
import shutil

from pyspark.sql import DataFrame, SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_INPUT = PROJECT_ROOT / "data" / "gold"
EXPORT_OUTPUT = PROJECT_ROOT / "data" / "exports" / "power_bi"

GOLD_DATASETS = [
    "job_market_overview",
    "jobs_by_source",
    "jobs_by_job_type",
    "jobs_by_functional_area",
    "salary_by_functional_area",
    "top_skills",
    "jobs_by_location_value",
    "jobs_posted_by_month",
]


def export_dataset(
    spark: SparkSession,
    dataset_name: str,
) -> int:
    """Export one Gold Parquet dataset to one Power BI friendly CSV file."""

    parquet_path = GOLD_INPUT / dataset_name

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Gold dataset does not exist: {parquet_path}. "
            "Run spark_jobs/build_gold.py first."
        )

    final_csv = EXPORT_OUTPUT / f"{dataset_name}.csv"
    temp_directory = EXPORT_OUTPUT / f".tmp_{dataset_name}"

    dataframe: DataFrame = spark.read.parquet(str(parquet_path))
    expected_rows = dataframe.count()

    if temp_directory.exists():
        shutil.rmtree(temp_directory)

    if final_csv.exists():
        final_csv.unlink()

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("encoding", "UTF-8")
        .csv(str(temp_directory))
    )

    part_files = list(temp_directory.glob("part-*.csv"))

    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one CSV part for {dataset_name}, "
            f"found {len(part_files)}. "
            f"Temporary output: {temp_directory}"
        )

    shutil.move(str(part_files[0]), str(final_csv))
    shutil.rmtree(temp_directory, ignore_errors=True)

    saved_rows = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .csv(str(final_csv))
        .count()
    )

    if saved_rows != expected_rows:
        raise RuntimeError(
            f"CSV verification failed for {dataset_name}: "
            f"expected {expected_rows} rows, saved {saved_rows}."
        )

    print(f"{dataset_name}: {saved_rows} rows verified")
    print(f"  File: {final_csv}")

    return saved_rows


def main() -> None:
    if not GOLD_INPUT.exists():
        raise FileNotFoundError(
            f"Gold directory does not exist: {GOLD_INPUT}. "
            "Run spark_jobs/build_gold.py first."
        )

    EXPORT_OUTPUT.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("PakistanJobMarketGoldPowerBIExport")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        print("Exporting Gold datasets to CSV for Power BI...\n")

        exported_rows = {}

        for dataset_name in GOLD_DATASETS:
            exported_rows[dataset_name] = export_dataset(
                spark,
                dataset_name,
            )

        print("\nPower BI export completed successfully.")
        print(f"Export directory: {EXPORT_OUTPUT}")
        print(f"CSV files created: {len(exported_rows)}")
        print(
            "Every exported CSV was read back and its row count verified."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
