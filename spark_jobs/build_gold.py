from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_INPUT = PROJECT_ROOT / "data" / "silver" / "jobs"
GOLD_OUTPUT = PROJECT_ROOT / "data" / "gold"


def write_and_verify(
    spark: SparkSession,
    df: DataFrame,
    dataset_name: str,
) -> int:
    """Write a Gold dataset to Parquet and verify its saved row count."""

    output_path = GOLD_OUTPUT / dataset_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    expected_count = df.count()
    df.write.mode("overwrite").parquet(str(output_path))

    saved_count = spark.read.parquet(str(output_path)).count()
    if saved_count != expected_count:
        raise RuntimeError(
            f"Verification failed for {dataset_name}: "
            f"expected {expected_count} rows, saved {saved_count}."
        )

    print(f"{dataset_name}: {saved_count} rows verified at {output_path}")
    return saved_count


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PakistanJobMarketGold")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        if not SILVER_INPUT.exists():
            raise FileNotFoundError(
                f"Silver input does not exist: {SILVER_INPUT}. "
                "Run spark_jobs/build_silver.py first."
            )

        silver = spark.read.parquet(str(SILVER_INPUT))
        silver_count = silver.count()

        print(f"Silver input rows: {silver_count}")
        print("Building Gold analytics datasets...\n")

        # 1. Overall market snapshot. Counts use the saved Silver records.
        job_market_overview = silver.agg(
            F.count(F.lit(1)).alias("total_job_records"),
            F.countDistinct("job_title").alias("distinct_job_titles"),
            F.countDistinct("source_name").alias("source_count"),
            F.countDistinct("job_type").alias("distinct_job_types"),
            F.countDistinct("location").alias("distinct_location_values"),
            F.count("salary_min").alias("records_with_salary_min"),
            F.count("salary_max").alias("records_with_salary_max"),
            F.count("company_name").alias("records_with_company_name"),
            F.count("posted_at_timestamp").alias(
                "records_with_posted_timestamp"
            ),
        ).withColumn("generated_on", F.current_date())

        # 2. Compare source coverage and data availability.
        jobs_by_source = (
            silver
            .groupBy("source_name")
            .agg(
                F.count(F.lit(1)).alias("total_job_records"),
                F.count("salary_min").alias("records_with_salary"),
                F.count("company_name").alias("records_with_company_name"),
                F.count("posted_at_timestamp").alias(
                    "records_with_posted_timestamp"
                ),
            )
            .withColumn(
                "salary_availability_pct",
                F.round(
                    F.col("records_with_salary")
                    / F.col("total_job_records") * F.lit(100),
                    2,
                ),
            )
            .orderBy(F.desc("total_job_records"))
        )

        # 3. Job type distribution.
        jobs_by_job_type = (
            silver
            .filter(F.col("job_type").isNotNull())
            .groupBy("job_type")
            .agg(
                F.count(F.lit(1)).alias("job_record_count"),
                F.countDistinct("source_name").alias("source_count"),
            )
            .orderBy(F.desc("job_record_count"))
        )

        # 4. Job count by functional area. Null areas are excluded.
        jobs_by_functional_area = (
            silver
            .filter(F.col("functional_area").isNotNull())
            .groupBy("functional_area")
            .agg(F.count(F.lit(1)).alias("job_record_count"))
            .orderBy(F.desc("job_record_count"))
        )

        # 5. Salary summaries by functional area. Salary values are PKR
        # and only monthly salary records are included for comparison.
        salary_by_functional_area = (
            silver
            .filter(
                F.col("functional_area").isNotNull()
                & F.col("salary_min").isNotNull()
                & (F.col("salary_period") == "month")
            )
            .groupBy("functional_area")
            .agg(
                F.count(F.lit(1)).alias("salary_record_count"),
                F.round(F.avg("salary_min"), 2).alias("average_salary_min"),
                F.round(F.avg("salary_max"), 2).alias("average_salary_max"),
                F.expr("percentile_approx(salary_min, 0.5)").alias(
                    "median_salary_min"
                ),
                F.expr("percentile_approx(salary_max, 0.5)").alias(
                    "median_salary_max"
                ),
            )
            .orderBy(F.desc("salary_record_count"))
        )

        # 6. Skill frequencies. Each skill is counted at most once per
        # Silver record, and matching is case insensitive.
        skills = (
            silver
            .filter(F.col("skills_raw").isNotNull())
            .select(
                "source_name",
                F.explode(
                    F.array_distinct(
                        F.split(F.col("skills_raw"), r"\s*,\s*")
                    )
                ).alias("skill"),
            )
            .withColumn("skill", F.lower(F.trim(F.col("skill"))))
            .filter(F.col("skill").isNotNull() & (F.col("skill") != ""))
        )

        top_skills = (
            skills
            .groupBy("skill")
            .agg(
                F.count(F.lit(1)).alias("job_records_with_skill"),
                F.countDistinct("source_name").alias("source_count"),
            )
            .orderBy(F.desc("job_records_with_skill"), F.asc("skill"))
        )

        # 7. Counts by observed location string. Note that source values can
        # contain several cities, so these are location values, not city-level
        # counts. City normalization can be added as a separate transformation.
        jobs_by_location_value = (
            silver
            .filter(F.col("location").isNotNull())
            .groupBy("location")
            .agg(F.count(F.lit(1)).alias("job_record_count"))
            .orderBy(F.desc("job_record_count"))
        )

        # 8. Posting trends use records that have a parsed posting timestamp.
        # In the current source data, Rozee does not provide a posting timestamp,
        # so this chart will currently represent only dated records from
        # Pakistan Jobs rather than both sources.
        jobs_posted_by_month = (
            silver
            .filter(F.col("posted_at_timestamp").isNotNull())
            .withColumn(
                "posted_month",
                F.date_format(F.col("posted_at_timestamp"), "yyyy-MM"),
            )
            .groupBy("posted_month", "source_name")
            .agg(F.count(F.lit(1)).alias("job_record_count"))
            .orderBy("posted_month", "source_name")
        )

        outputs = [
            ("job_market_overview", job_market_overview),
            ("jobs_by_source", jobs_by_source),
            ("jobs_by_job_type", jobs_by_job_type),
            ("jobs_by_functional_area", jobs_by_functional_area),
            ("salary_by_functional_area", salary_by_functional_area),
            ("top_skills", top_skills),
            ("jobs_by_location_value", jobs_by_location_value),
            ("jobs_posted_by_month", jobs_posted_by_month),
        ]

        print("Gold output verification")
        for dataset_name, dataframe in outputs:
            write_and_verify(spark, dataframe, dataset_name)

        print("\nGold layer completed successfully.")
        print(f"Gold output directory: {GOLD_OUTPUT}")
        print(
            "Note: jobs_posted_by_month includes only records with a valid "
            "posting timestamp. Location summaries group the source location "
            "string as provided and do not split multi-city values."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
