
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import logging
import shutil
import uuid

import pandas as pd


def configure_logging(project_root: Path) -> logging.Logger:
    """Configure shared pipeline logging."""

    log_directory = project_root / "logs"
    log_directory.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("job_market_ingestion")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(
            log_directory / "ingestion.log",
            encoding="utf8",
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


def calculate_sha256(file_path: Path) -> str:
    """Calculate a fingerprint for a file."""

    digest = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def find_existing_successful_ingestion(
    bronze_root: Path,
    source_checksum: str,
) -> Path | None:
    """Find a verified successful ingestion of identical content."""

    if not bronze_root.exists():
        return None

    for metadata_path in bronze_root.rglob("ingestion_metadata.json"):
        try:
            metadata = json.loads(
                metadata_path.read_text(encoding="utf8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            logging.getLogger("job_market_ingestion").warning(
                "Unable to read metadata at %s: %s",
                metadata_path,
                exc,
            )
            continue

        if metadata.get("sha256") != source_checksum:
            continue

        if metadata.get("status") != "success":
            continue

        bronze_file_name = metadata.get("bronze_file_name")

        if not bronze_file_name:
            continue

        bronze_file = metadata_path.parent / bronze_file_name

        if not bronze_file.is_file():
            continue

        if calculate_sha256(bronze_file) != source_checksum:
            continue

        return metadata_path

    return None


def ingest_csv(
    source_name: str,
    source_path: Path,
    bronze_root: Path,
    expected_columns: list[str],
    bronze_file_name: str,
    logger: logging.Logger,
) -> dict:
    """Validate and copy one CSV source into Bronze."""

    started_at = datetime.now(timezone.utc)
    run_id = uuid.uuid4().hex

    staging_directory = None

    logger.info(
        "Starting %s ingestion. Run ID: %s",
        source_name,
        run_id,
    )

    try:
        if not source_path.is_file():
            raise FileNotFoundError(
                f"Source file not found: {source_path}"
            )

        logger.info("Source file found: %s", source_path.name)

        df = pd.read_csv(source_path)

        if df.empty:
            raise ValueError("Source file contains no records.")

        missing_columns = [
            column
            for column in expected_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Required columns missing: {missing_columns}"
            )

        logger.info(
            "Validation successful. Rows: %s, Columns: %s",
            len(df),
            len(df.columns),
        )

        source_checksum = calculate_sha256(source_path)

        existing_metadata = find_existing_successful_ingestion(
            bronze_root,
            source_checksum,
        )

        if existing_metadata is not None:
            logger.info(
                "Identical source content already ingested. "
                "Skipping duplicate ingestion."
            )

            result = {
                "run_id": run_id,
                "source_name": source_name,
                "source_file_name": source_path.name,
                "sha256": source_checksum,
                "status": "skipped",
                "reason": "Identical content already exists in Bronze",
                "existing_metadata_path": str(existing_metadata),
            }

            return result

        
        date_folder = started_at.strftime("%Y_%m_%d")

        date_directory = (
            bronze_root / f"ingestion_date={date_folder}"
        )
        date_directory.mkdir(parents=True, exist_ok=True)

        bronze_directory = (
            date_directory
            / f"batch_{source_checksum[:12]}"
        )

        if bronze_directory.exists():
            raise FileExistsError(
                f"Bronze batch already exists: {bronze_directory}"
            )

        staging_directory = (
            date_directory / f".staging_{run_id}"
        )
        staging_directory.mkdir()

        bronze_file = staging_directory / bronze_file_name

        shutil.copy2(source_path, bronze_file)

        bronze_checksum = calculate_sha256(bronze_file)

        if source_checksum != bronze_checksum:
            raise IOError(
                "Checksum mismatch between source and Bronze copy."
            )

        completed_at = datetime.now(timezone.utc)

        metadata = {
            "run_id": run_id,
            "source_name": source_name,
            "source_file_name": source_path.name,
            "bronze_file_name": bronze_file.name,
            "ingestion_started_at": started_at.isoformat(),
            "ingestion_completed_at": completed_at.isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "file_size_bytes": source_path.stat().st_size,
            "sha256": source_checksum,
            "status": "success",
        }

        metadata_path = (
            staging_directory / "ingestion_metadata.json"
        )

        metadata_path.write_text(
            json.dumps(metadata, indent=2),
            encoding="utf8",
        )

        # Publish only after the CSV and metadata are ready.
        if bronze_directory.exists():
            raise FileExistsError(
                f"Bronze batch already exists: {bronze_directory}"
            )

        staging_directory.rename(bronze_directory)
        staging_directory = None

        bronze_file = bronze_directory / bronze_file_name
        metadata_path = bronze_directory / "ingestion_metadata.json"

        logger.info("Bronze file saved: %s", bronze_file)
        logger.info("Metadata saved: %s", metadata_path)
        logger.info(
            "%s ingestion completed successfully.",
            source_name,
        )

        return metadata
   
    except Exception:
        if (
            staging_directory is not None
            and staging_directory.exists()
        ):
            try:
                shutil.rmtree(staging_directory)
            except OSError:
                logger.exception(
                    "Could not remove staging directory: %s",
                    staging_directory,
                )

        logger.exception("%s ingestion failed.", source_name)
        raise

