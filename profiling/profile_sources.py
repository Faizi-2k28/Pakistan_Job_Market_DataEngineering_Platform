from pathlib import Path
import pandas as pd


# This finds the project root using the location of the 
# Python script. Our file paths can therefore work even 
# when the terminal is opened from a different directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCES={
    "rozee": (
        PROJECT_ROOT
        /"data"
        /"source"
        /"rozee"
        /"RozeePK_Jobs.csv"
    ),
    "pakistan_jobs": (
        PROJECT_ROOT
        /"data"
        /"source"
        /"pakistan_jobs"
        /"pakistan_jobs.csv"
    ),
}

def profile_source(source_name: str, file_path: Path) -> None:
    print(f"\n{'=' * 50}")
    print(f"Source:{source_name}")
    print(f"File:{file_path.name}")

    if not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return

    df = pd.read_csv(file_path)

    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")

    print(f"\nColumn names:")

    for column in df.columns:
        print(f"{column}")

    print(f"\nData types:")
    print(df.dtypes.to_string())

    print(f"\nMissing values per column:")
    missing_values = df.isna().sum()
    print(missing_values.to_string())

    print(f"\nMissing Percentage per column:")
    missing_percentage = (df.isna().mean() * 100).round(2)
    print(missing_percentage.to_string())

    print(f"\nFully duplicated rows:")
    print(df.duplicated().sum())

    
    non_empty_df = df.loc[~df.isna().all(axis=1)].copy()

    print("\nCompletely empty rows:")
    print(df.isna().all(axis=1).sum())

    print("\nExact duplicates among nonempty rows:")
    print(non_empty_df.duplicated().sum())

    title_column = "Title" if "Title" in df.columns else "jobTitle"

    repeated_titles = non_empty_df[
        non_empty_df.duplicated(
            subset=[title_column],
            keep=False
        )
    ]

    print("\nRecords with repeated job titles:")
    print(len(repeated_titles))

    if not repeated_titles.empty:
        print(
            repeated_titles[title_column]
            .value_counts()
            .head(10)
            .to_string()
        )

    print(f"\nFirst three records:")
    print(df.head(3).to_string(index=False))

    print(f"\nRows with all fields missing:")
    print(df.isna().all(axis=1).sum())

    print(f"\nDistribution of missing values per row:")
    missing_per_row = df.isna().sum(axis=1)
    print(missing_per_row.value_counts().sort_index().to_string())

    print(f"\nRepeated records sample:")
    duplicated_rows = df[df.duplicated(keep=False)]

    if duplicated_rows.empty:
        print("No exact duplicate rows found.")
    else:
        title_column =(
            "Title" if "Title" in df.columns else "jobTitle"
        )

        display_columns = [title_column]

        if " Job Location" in df.columns:
            display_columns.append(" Job Location")

        if "location" in df.columns:
            display_columns.append("location")

        if "companyName" in df.columns:
            display_columns.append("companyName")

        print(
            duplicated_rows[display_columns]
            .head(10)
            .to_string(index=False)
        )

    categorical_columns = {
        "rozee": [
            "Job Type",
            "Functional Area",
            "Career Level",
            "Minimum Education",
            "Gender",
        ],
        "pakistan_jobs": ["location", "jobType"],
    }

    print("\nCategorical value distribution:")

    for column in categorical_columns[source_name]:
        if column in df.columns:
            print(f"\n{column}:")
            print(
                df[column]
                .value_counts(dropna=False)
                .head(10)
                .to_string()
            )



def main():
    for source_name, file_path in SOURCES.items():
        profile_source(source_name, file_path)

if __name__ == "__main__":
    main()