from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

PROJECT_ROOT = Path(__file__).resolve().parents[1]

ROZEE_BRONZE = PROJECT_ROOT / "data" / "bronze" / "rozee" 
PAKISTAN_JOBS = PROJECT_ROOT / "data" / "bronze" / "pakistan_jobs" 

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

def build_string_schema(columns: list[str]) -> StructType:
    """Create an explicit scehma with strings columns."""

    return StructType([
        StructField(column, StringType(), True)
        for column in columns
    ])

def find_csv_files(directory: Path) -> list[Path]:
    """Find CSV files under Bronze directory."""

    files = sorted(directory.rglob("*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No CSV files found under {directory}"
        )
    return files

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PakistanJobMarketBronzeReader")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        rozee_files = find_csv_files(ROZEE_BRONZE)
        pakistan_files = find_csv_files(PAKISTAN_JOBS)

        print("Rozee Bronze Files")
        for file_path in rozee_files:
           print(file_path)

        print("Pakistan Jobs Bronze files")
        for file_path in pakistan_files:
            print(file_path)

        
        rozee_df = spark.read.csv(
            [str(path) for path in rozee_files],
            schema=build_string_schema(ROZEE_COLUMNS),
            header=True,
            multiLine=True,
        )

        
        pakistan_df = spark.read.csv(
            [str(path) for path in pakistan_files],
            schema=build_string_schema(PAKISTAN_COLUMNS),
            header=True,
            multiLine=True,
        )

        print("\nROZEE DATA")
        print("columns:", len(rozee_df.columns))
        print("rows:", rozee_df.count())
        rozee_df.printSchema()
        rozee_df.show(3, truncate=60)

        print("\nPAKISTAN JOBS DATA")
        print("Columns:", len(pakistan_df.columns))
        print("Rows:", pakistan_df.count())
        pakistan_df.printSchema()

        print("\nPakistan Jobs preview:")
        pakistan_df.select(
            "jobTitle",
            "companyName",
            "postingDate",
            "location",
            "jobType",
            "skills",
        ).show(3, truncate=60)
    finally:
        spark.stop()

if __name__ =="__main__":
    main()


