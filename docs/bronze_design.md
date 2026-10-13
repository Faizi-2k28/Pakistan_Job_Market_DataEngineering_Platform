
# Bronze Layer Design

## 1. Purpose

Preserve the original input data received from each job source and maintain traceability for every ingestion run.

## 2. Storage Format

Preserve the original CSV representation in Bronze.

Do not perform business transformations during ingestion.

## 3. Directory Strategy

Separate data by source and ingestion date.

Each accepted batch must have its own identifiable storage location.

## 4. File Metadata

Record the source name, source file name, ingestion run identifier, ingestion timestamps, file size, SHA256 checksum, row count, column count, and processing status where available.

## 5. Data Preservation

Keep the source directory unchanged.

Copy accepted source files into Bronze without rewriting their content.

## 6. Rerun Strategy

Calculate a SHA256 checksum to identify unchanged input files.

Do not silently create duplicate batches when the same input is processed again.

Record whether a run succeeded, failed, or was skipped because the input was already processed.

## 7. Data Protection

Keep source data, Bronze files, runtime metadata, and logs out of the public GitHub repository.

Do not expose recruiter contact information in public examples or analytical outputs.

## 8. Future Processing

Apache Spark will read the Bronze data, perform cleaning and standardization, and write the Silver layer.

Parquet will be introduced during the processing stage.
