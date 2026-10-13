import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[1]

ROZEE_BRONZE = PROJECT_ROOT / "data" / "bronze" / "rozee"
PAKISTAN_BRONZE = PROJECT_ROOT / "data" / "bronze" / "pakistan_jobs"
JOBSPIPE_BRONZE = PROJECT_ROOT / "data" / "bronze" / "jobspipe"

SILVER_OUTPUT = PROJECT_ROOT / "data" / "silver" / "jobs"
QUARANTINE_OUTPUT = PROJECT_ROOT / "data" / "quarantine" / "missing_job_title"

ROZEE_COLUMNS = [
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

# Keep this list in the exact order of the source CSV header.
PAKISTAN_COLUMNS = [
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

JOBSPIPE_COLUMNS = [
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

ROZEE_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in ROZEE_COLUMNS]
)
PAKISTAN_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in PAKISTAN_COLUMNS]
)
JOBSPIPE_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in JOBSPIPE_COLUMNS]
)

# Columns common to all sources before parsed analytics fields are added.
# Source-specific salary fields are carried as helper values and converted
# into canonical salary_min, salary_max, salary_currency and salary_period.
SILVER_BASE_COLUMNS = [
    "job_title",
    "company_name",
    "location",
    "salary_raw",
    "job_type",
    "functional_area",
    "industry",
    "career_level",
    "experience_requirement",
    "education_requirement",
    "gender_requirement",
    "age_requirement",
    "skills_raw",
    "posted_at",
    "application_deadline",
    "job_url",
    "source_name",
    "source_file_name",
    "source_job_id",
    "country",
    "country_code",
    "upstream_source",
    "source_discovered_at",
    "source_collected_at",
    "salary_min_input",
    "salary_max_input",
    "salary_currency_input",
    "salary_period_input",
]

SALARY_RANGE_PATTERN = r"(?i)([\d,]+)\s*(?:-|–|to)\s*([\d,]+)"
OPEN_ENDED_SALARY_PATTERN = r"(?i)([\d,]+)\s*\+"


def _parse_metadata_timestamp(value: str | None) -> float:
    """Convert an ISO timestamp to epoch seconds for comparison."""
    if not value:
        return 0.0
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError):
        return 0.0


def find_latest_successful_ingestion(
    directory: Path,
    expected_source_name: str,
) -> tuple[Path, dict, Path]:
    """Select the latest successful full-snapshot CSV using Bronze metadata."""
    if not directory.exists():
        raise FileNotFoundError(
            f"Bronze source directory does not exist: {directory}"
        )

    candidates: list[tuple[float, Path, dict, Path]] = []

    for metadata_path in sorted(directory.rglob("ingestion_metadata.json")):
        try:
            with metadata_path.open("r", encoding="utf-8") as file:
                metadata = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Skipping unreadable metadata {metadata_path}: {exc}")
            continue

        if metadata.get("status") != "success":
            continue
        if metadata.get("source_name") != expected_source_name:
            continue

        bronze_file_name = (
            metadata.get("bronze_file_name")
            or metadata.get("source_file_name")
        )
        if not bronze_file_name:
            print(f"Skipping metadata without a Bronze filename: {metadata_path}")
            continue

        csv_path = metadata_path.parent / bronze_file_name
        if not csv_path.is_file():
            print(f"Skipping metadata with missing CSV: {csv_path}")
            continue

        candidates.append(
            (
                _parse_metadata_timestamp(metadata.get("ingestion_completed_at")),
                csv_path,
                metadata,
                metadata_path,
            )
        )

    if not candidates:
        raise FileNotFoundError(
            "No successful Bronze ingestion with a readable CSV was found "
            f"for source '{expected_source_name}' under {directory}"
        )

    _, csv_path, metadata, metadata_path = max(
        candidates, key=lambda candidate: candidate[0]
    )
    return csv_path, metadata, metadata_path


def read_source_csv(
    spark: SparkSession,
    file_path: Path,
    schema: StructType,
) -> DataFrame:
    """Read a single Bronze CSV with an explicit schema and multiline support."""
    return (
        spark.read
        .schema(schema)
        .option("header", True)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .csv(str(file_path))
    )


def normalize_text(df: DataFrame) -> DataFrame:
    """Trim text, normalize whitespace, and convert common null markers."""
    cleaned_df = df
    for column_name in df.columns:
        original_value = F.col(f"`{column_name}`")
        cleaned_value = F.trim(
            F.regexp_replace(
                F.regexp_replace(original_value, r"[\r\n\t]+", " "),
                r"\s+",
                " ",
            )
        )
        cleaned_df = cleaned_df.withColumn(
            column_name,
            F.when(
                cleaned_value.isNull()
                | (cleaned_value == "")
                | F.upper(cleaned_value).isin("NA", "N/A", "NULL"),
                F.lit(None).cast(StringType()),
            ).otherwise(cleaned_value),
        )
    return cleaned_df


def _common_null_columns() -> dict:
    """Return typed null expressions for source-optional Silver fields."""
    return {
        "salary_min_input": F.lit(None).cast(StringType()),
        "salary_max_input": F.lit(None).cast(StringType()),
        "salary_currency_input": F.lit(None).cast(StringType()),
        "salary_period_input": F.lit(None).cast(StringType()),
        "source_job_id": F.lit(None).cast(StringType()),
        "country": F.lit(None).cast(StringType()),
        "country_code": F.lit(None).cast(StringType()),
        "upstream_source": F.lit(None).cast(StringType()),
        "source_discovered_at": F.lit(None).cast(StringType()),
        "source_collected_at": F.lit(None).cast(StringType()),
    }


def standardize_rozee(df: DataFrame) -> DataFrame:
    """Map Rozee fields into the shared Silver schema."""
    df = normalize_text(df)
    extra = _common_null_columns()
    return df.select(
        F.col("`Title`").alias("job_title"),
        F.lit(None).cast(StringType()).alias("company_name"),
        F.col("`Job Location`").alias("location"),
        F.col("`Salary`").alias("salary_raw"),
        F.col("`Job Type`").alias("job_type"),
        F.col("`Functional Area`").alias("functional_area"),
        F.lit(None).cast(StringType()).alias("industry"),
        F.col("`Career Level`").alias("career_level"),
        F.col("`Minimum Experience`").alias("experience_requirement"),
        F.col("`Minimum Education`").alias("education_requirement"),
        F.col("`Gender`").alias("gender_requirement"),
        F.col("`Age`").alias("age_requirement"),
        F.col("`Skills`").alias("skills_raw"),
        F.lit(None).cast(StringType()).alias("posted_at"),
        F.col("`Apply Before`").alias("application_deadline"),
        F.lit(None).cast(StringType()).alias("job_url"),
        F.lit("rozee").alias("source_name"),
        F.lit("RozeePK_Jobs.csv").alias("source_file_name"),
        extra["source_job_id"].alias("source_job_id"),
        extra["country"].alias("country"),
        extra["country_code"].alias("country_code"),
        extra["upstream_source"].alias("upstream_source"),
        extra["source_discovered_at"].alias("source_discovered_at"),
        extra["source_collected_at"].alias("source_collected_at"),
        extra["salary_min_input"].alias("salary_min_input"),
        extra["salary_max_input"].alias("salary_max_input"),
        extra["salary_currency_input"].alias("salary_currency_input"),
        extra["salary_period_input"].alias("salary_period_input"),
    )


def standardize_pakistan_jobs(df: DataFrame) -> DataFrame:
    """Map Pakistan Jobs fields while excluding recruiter contact information."""
    df = normalize_text(df)
    extra = _common_null_columns()
    return df.select(
        F.col("`jobTitle`").alias("job_title"),
        F.col("`companyName`").alias("company_name"),
        F.col("`location`").alias("location"),
        F.col("`salaryRange`").alias("salary_raw"),
        F.col("`jobType`").alias("job_type"),
        F.lit(None).cast(StringType()).alias("functional_area"),
        F.col("`industry`").alias("industry"),
        F.lit(None).cast(StringType()).alias("career_level"),
        F.lit(None).cast(StringType()).alias("experience_requirement"),
        F.lit(None).cast(StringType()).alias("education_requirement"),
        F.lit(None).cast(StringType()).alias("gender_requirement"),
        F.lit(None).cast(StringType()).alias("age_requirement"),
        F.col("`skills`").alias("skills_raw"),
        F.col("`postingDate`").alias("posted_at"),
        F.lit(None).cast(StringType()).alias("application_deadline"),
        F.col("`url`").alias("job_url"),
        F.lit("pakistan_jobs").alias("source_name"),
        F.lit("pakistan_jobs.csv").alias("source_file_name"),
        extra["source_job_id"].alias("source_job_id"),
        extra["country"].alias("country"),
        extra["country_code"].alias("country_code"),
        extra["upstream_source"].alias("upstream_source"),
        extra["source_discovered_at"].alias("source_discovered_at"),
        extra["source_collected_at"].alias("source_collected_at"),
        extra["salary_min_input"].alias("salary_min_input"),
        extra["salary_max_input"].alias("salary_max_input"),
        extra["salary_currency_input"].alias("salary_currency_input"),
        extra["salary_period_input"].alias("salary_period_input"),
    )


def standardize_jobspipe(df: DataFrame) -> DataFrame:
    """Map JobsPipe records, retaining provider ID, source and salary metadata."""
    df = normalize_text(df)
    return df.select(
        F.col("job_title").alias("job_title"),
        F.col("company_name").alias("company_name"),
        F.col("location").alias("location"),
        F.col("salary_string").alias("salary_raw"),
        F.col("employment_type").alias("job_type"),
        F.lit(None).cast(StringType()).alias("functional_area"),
        F.lit(None).cast(StringType()).alias("industry"),
        F.col("seniority").alias("career_level"),
        F.lit(None).cast(StringType()).alias("experience_requirement"),
        F.lit(None).cast(StringType()).alias("education_requirement"),
        F.lit(None).cast(StringType()).alias("gender_requirement"),
        F.lit(None).cast(StringType()).alias("age_requirement"),
        F.col("skills").alias("skills_raw"),
        F.col("date_posted").alias("posted_at"),
        F.lit(None).cast(StringType()).alias("application_deadline"),
        F.col("job_url").alias("job_url"),
        F.lit("jobspipe").alias("source_name"),
        F.lit("jobspipe_jobs.csv").alias("source_file_name"),
        F.col("jobspipe_id").alias("source_job_id"),
        F.col("country").alias("country"),
        F.upper(F.col("country_code")).alias("country_code"),
        F.col("upstream_source").alias("upstream_source"),
        F.col("discovered_at").alias("source_discovered_at"),
        F.col("collected_at").alias("source_collected_at"),
        F.col("min_annual_salary").alias("salary_min_input"),
        F.col("max_annual_salary").alias("salary_max_input"),
        F.col("salary_currency").alias("salary_currency_input"),
        F.when(
            F.col("min_annual_salary").isNotNull()
            | F.col("max_annual_salary").isNotNull(),
            F.lit("year"),
        ).otherwise(F.lit(None).cast(StringType())).alias("salary_period_input"),
    )


def add_analytics_columns(df: DataFrame) -> DataFrame:
    """Create typed salary/date fields while preserving raw values and units."""
    df = (
        df
        .withColumn(
            "_salary_min_text",
            F.regexp_extract(F.col("salary_raw"), SALARY_RANGE_PATTERN, 1),
        )
        .withColumn(
            "_salary_max_text",
            F.regexp_extract(F.col("salary_raw"), SALARY_RANGE_PATTERN, 2),
        )
        .withColumn(
            "_open_ended_salary_min_text",
            F.regexp_extract(F.col("salary_raw"), OPEN_ENDED_SALARY_PATTERN, 1),
        )
        .withColumn(
            "salary_min",
            F.when(
                F.col("_salary_min_text") != "",
                F.regexp_replace(F.col("_salary_min_text"), ",", "").cast("long"),
            )
            .when(
                F.col("_open_ended_salary_min_text") != "",
                F.regexp_replace(
                    F.col("_open_ended_salary_min_text"), ",", ""
                ).cast("long"),
            )
            .otherwise(F.col("salary_min_input").cast("long")),
        )
        .withColumn(
            "salary_max",
            F.when(
                F.col("_salary_max_text") != "",
                F.regexp_replace(F.col("_salary_max_text"), ",", "").cast("long"),
            ).otherwise(F.col("salary_max_input").cast("long")),
        )
        .withColumn(
            "salary_currency",
            F.when(
                F.col("salary_currency_input").isNotNull()
                & (F.trim(F.col("salary_currency_input")) != ""),
                F.upper(F.trim(F.col("salary_currency_input"))),
            )
            .when(F.upper(F.col("salary_raw")).contains("PKR"), F.lit("PKR"))
            .otherwise(F.lit(None).cast(StringType())),
        )
        .withColumn(
            "salary_period",
            F.when(
                F.col("salary_period_input").isNotNull(),
                F.col("salary_period_input"),
            )
            .when(F.lower(F.col("salary_raw")).rlike(r"/\s*month\b"), "month")
            .when(F.lower(F.col("salary_raw")).rlike(r"/\s*year\b"), "year")
            .when(F.lower(F.col("salary_raw")).rlike(r"/\s*week\b"), "week")
            .when(F.lower(F.col("salary_raw")).rlike(r"/\s*day\b"), "day")
            .otherwise(F.lit(None).cast(StringType())),
        )
        .withColumn(
            "application_deadline_date",
            F.try_to_timestamp(
                F.col("application_deadline"), F.lit("d-MMM-yy")
            ).cast("date"),
        )
        .withColumn(
            "posted_at_timestamp",
            F.try_to_timestamp(F.col("posted_at")),
        )
        .drop(
            "_salary_min_text",
            "_salary_max_text",
            "_open_ended_salary_min_text",
            "salary_min_input",
            "salary_max_input",
            "salary_currency_input",
            "salary_period_input",
        )
    )
    return df


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PakistanJobMarketSilver")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        # Step 1: Select the newest successful full snapshot for each source.
        rozee_file, rozee_metadata, rozee_metadata_path = (
            find_latest_successful_ingestion(ROZEE_BRONZE, "rozee")
        )
        pakistan_file, pakistan_metadata, pakistan_metadata_path = (
            find_latest_successful_ingestion(PAKISTAN_BRONZE, "pakistan_jobs")
        )
        jobspipe_file, jobspipe_metadata, jobspipe_metadata_path = (
            find_latest_successful_ingestion(JOBSPIPE_BRONZE, "jobspipe")
        )

        print("Selected Bronze snapshots")
        for name, path, metadata, metadata_path in [
            ("Rozee", rozee_file, rozee_metadata, rozee_metadata_path),
            ("Pakistan Jobs", pakistan_file, pakistan_metadata, pakistan_metadata_path),
            ("JobsPipe", jobspipe_file, jobspipe_metadata, jobspipe_metadata_path),
        ]:
            print(f"{name} CSV: {path}")
            print(f"  Ingestion completed: {metadata.get('ingestion_completed_at')}")
            print(f"  Metadata: {metadata_path}")

        # Step 2: Read only the selected snapshots.
        rozee_raw = read_source_csv(spark, rozee_file, ROZEE_SCHEMA)
        pakistan_raw = read_source_csv(spark, pakistan_file, PAKISTAN_SCHEMA)
        jobspipe_raw = read_source_csv(spark, jobspipe_file, JOBSPIPE_SCHEMA)

        rozee_raw_count = rozee_raw.count()
        pakistan_raw_count = pakistan_raw.count()
        jobspipe_raw_count = jobspipe_raw.count()

        print("\nRaw source counts")
        print(f"Rozee: {rozee_raw_count}")
        print(f"Pakistan Jobs: {pakistan_raw_count}")
        print(f"JobsPipe: {jobspipe_raw_count}")

        # Step 3: Standardize and combine all three sources.
        rozee_standardized = standardize_rozee(rozee_raw)
        pakistan_standardized = standardize_pakistan_jobs(pakistan_raw)
        jobspipe_standardized = standardize_jobspipe(jobspipe_raw)

        combined = (
            rozee_standardized
            .unionByName(pakistan_standardized)
            .unionByName(jobspipe_standardized)
        )
        total_candidate_count = combined.count()

        # Step 4: Quarantine rows without job titles.
        rejected_records = (
            combined
            .filter(F.col("job_title").isNull())
            .withColumn("rejection_reason", F.lit("missing_job_title"))
        )
        rejected_count = rejected_records.count()

        # Step 5: Keep records with titles.
        valid_silver = combined.filter(F.col("job_title").isNotNull())
        valid_count_before_deduplication = valid_silver.count()

        # Step 6: Find exact duplicate groups using standardized fields.
        duplicate_groups_df = (
            valid_silver
            .groupBy(*SILVER_BASE_COLUMNS)
            .count()
            .filter(F.col("count") > 1)
        )
        duplicate_group_count = duplicate_groups_df.count()

        # Step 7: Deduplicate exact copies. The JobsPipe stable source_job_id
        # prevents distinct provider job IDs from collapsing together.
        silver_deduplicated = valid_silver.dropDuplicates(SILVER_BASE_COLUMNS)
        unique_silver_count = silver_deduplicated.count()
        duplicate_rows_removed = (
            valid_count_before_deduplication - unique_silver_count
        )

        # Step 8: Add canonical salary, currency, period and date columns.
        silver_enriched = add_analytics_columns(silver_deduplicated)

        print("\nData Quality Report")
        print(f"Total candidate records: {total_candidate_count}")
        print(f"Records missing job title: {rejected_count}")
        print(f"Valid records before deduplication: {valid_count_before_deduplication}")
        print(f"Exact duplicate groups: {duplicate_group_count}")
        print(f"Exact duplicate rows removed: {duplicate_rows_removed}")
        print(f"Unique Silver records: {unique_silver_count}")

        print("\nDuplicate records for review")
        if duplicate_group_count:
            duplicate_groups_df.show(truncate=False)
        else:
            print("No exact duplicate groups found.")

        print("\nEnriched Silver schema")
        silver_enriched.printSchema()

        print("\nSample JobsPipe Silver records")
        silver_enriched.filter(F.col("source_name") == "jobspipe").select(
            "source_job_id",
            "job_title",
            "company_name",
            "location",
            "salary_raw",
            "salary_min",
            "salary_max",
            "salary_currency",
            "salary_period",
            "posted_at_timestamp",
            "source_name",
        ).show(10, truncate=False)

        print("\nSample Silver records")
        silver_enriched.select(
            "job_title",
            "company_name",
            "location",
            "salary_raw",
            "salary_min",
            "salary_max",
            "salary_currency",
            "salary_period",
            "application_deadline_date",
            "posted_at_timestamp",
            "source_name",
        ).show(10, truncate=False)

        # Step 9: Parsing and data quality summaries.
        salary_ranges_parsed = silver_enriched.filter(
            F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull()
        ).count()
        open_ended_salaries_parsed = silver_enriched.filter(
            F.col("salary_min").isNotNull() & F.col("salary_max").isNull()
        ).count()
        salary_values_parsed = silver_enriched.filter(
            F.col("salary_min").isNotNull()
        ).count()
        nonempty_salaries_unparsed = silver_enriched.filter(
            F.col("salary_raw").isNotNull() & F.col("salary_min").isNull()
        ).count()
        deadlines_parsed = silver_enriched.filter(
            F.col("application_deadline_date").isNotNull()
        ).count()
        posting_timestamps_parsed = silver_enriched.filter(
            F.col("posted_at_timestamp").isNotNull()
        ).count()

        print("\nParsing Summary")
        print(f"Salary ranges parsed with minimum and maximum: {salary_ranges_parsed}")
        print(f"Open-ended or minimum-only salary values: {open_ended_salaries_parsed}")
        print(f"Salary values with a parsed minimum: {salary_values_parsed}")
        print(f"Nonempty salaries not parsed: {nonempty_salaries_unparsed}")
        print(f"Application deadlines parsed as dates: {deadlines_parsed}")
        print(f"Posting timestamps parsed: {posting_timestamps_parsed}")

        # Step 10: Prepare output directories and persist results.
        SILVER_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        QUARANTINE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

        silver_enriched.write.mode("overwrite").parquet(str(SILVER_OUTPUT))
        rejected_records.write.mode("overwrite").parquet(str(QUARANTINE_OUTPUT))

        # Step 11: Read saved outputs back and verify row counts.
        silver_readback = spark.read.parquet(str(SILVER_OUTPUT))
        quarantine_readback = spark.read.parquet(str(QUARANTINE_OUTPUT))

        silver_written_count = silver_readback.count()
        quarantine_written_count = quarantine_readback.count()

        print("\nOutput Verification")
        print(f"Silver Parquet rows: {silver_written_count}")
        print(f"Quarantine rows: {quarantine_written_count}")
        print(f"Silver output path: {SILVER_OUTPUT}")
        print(f"Quarantine output path: {QUARANTINE_OUTPUT}")

        if silver_written_count != unique_silver_count:
            raise RuntimeError(
                "Silver output verification failed: written rows do not "
                "match the deduplicated count."
            )
        if quarantine_written_count != rejected_count:
            raise RuntimeError(
                "Quarantine output verification failed: written rows do not "
                "match the rejected count."
            )

        print("\nSilver processing completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
