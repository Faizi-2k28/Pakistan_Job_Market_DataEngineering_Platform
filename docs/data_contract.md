
# Unified Job Data Contract

## 1. Purpose

Define the standardized structure for job records in the Silver layer of the Pakistan Job Market Data Engineering Platform.

The Bronze layer preserves original source records. The Silver layer standardizes fields from different sources according to this contract.

## 2. Source Mapping

### Job title

Canonical field: job_title

Rozee source: Title

Pakistan Jobs source: jobTitle

Type: String

Requirement: Required for a valid Silver record.

### Company name

Canonical field: company_name

Rozee source: Not available

Pakistan Jobs source: companyName

Type: String

Requirement: Optional.

### Location

Canonical field: location

Rozee source: Job Location

Pakistan Jobs source: location

Type: String

Requirement: Optional in the initial contract.

### Salary

Canonical field: salary_raw

Rozee source: Salary

Pakistan Jobs source: salaryRange

Type: String for the initial raw representation.

Requirement: Optional.

Note: Pakistan Jobs currently has no populated salary values. Salary parsing will be performed during transformation.

### Job type

Canonical field: job_type

Rozee source: Job Type

Pakistan Jobs source: jobType

Type: String

Requirement: Optional.

### Functional area

Canonical field: functional_area

Rozee source: Functional Area

Pakistan Jobs source: Not available

Type: String

Requirement: Optional.

### Industry

Canonical field: industry

Rozee source: Not available

Pakistan Jobs source: industry

Type: String

Requirement: Optional.

Note: The Pakistan Jobs industry field is entirely empty in the supplied dataset.

### Career level

Canonical field: career_level

Rozee source: Career Level

Pakistan Jobs source: Not available

Type: String

Requirement: Optional.

### Experience requirement

Canonical field: experience_requirement

Rozee source: Minimum Experience

Pakistan Jobs source: Not available

Type: String initially, with numerical interpretation handled later.

Requirement: Optional.

### Education requirement

Canonical field: education_requirement

Rozee source: Minimum Education

Pakistan Jobs source: Not available

Type: String

Requirement: Optional.

### Gender requirement

Canonical field: gender_requirement

Rozee source: Gender

Pakistan Jobs source: Not available

Type: String

Requirement: Optional.

### Age requirement

Canonical field: age_requirement

Rozee source: Age

Pakistan Jobs source: Not available

Type: String initially, with parsing handled later.

Requirement: Optional.

### Skills

Canonical field: skills_raw

Rozee source: Skills

Pakistan Jobs source: skills

Type: String initially.

Requirement: Optional.

Note: Skills will be standardized and separated into individual skills during transformation.

### Posting timestamp

Canonical field: posted_at

Rozee source: Not available

Pakistan Jobs source: postingDate

Type: Timestamp after validation and conversion.

Requirement: Optional.

### Application deadline

Canonical field: application_deadline

Rozee source: Apply Before

Pakistan Jobs source: Not available

Type: Date after validation and conversion.

Requirement: Optional.

### Job URL

Canonical field: job_url

Rozee source: Not available

Pakistan Jobs source: url

Type: String

Requirement: Optional.

## 3. Pipeline Metadata

### source

Identifies which dataset supplied the record.

Type: String

Requirement: Required.

### source_record_id

Identifies a particular record from a source snapshot.

Type: String

Requirement: Required for lineage.

Note: Neither supplied CSV has a reliable shared job identifier. We will design a stable record identification strategy before implementing incremental processing.

### ingested_at

Records when the pipeline ingested the source data.

Type: Timestamp

Requirement: Required.

### ingestion_run_id

Identifies the pipeline execution that processed the record.

Type: String

Requirement: Required.

## 4. Validation Rules

1. The source must be identifiable.
2. Every record must retain its source lineage.
3. Empty source rows must be identified before Silver records are created.
4. A valid Silver job record must have a nonempty job title.
5. Missing optional fields must remain missing rather than being assigned fabricated values.
6. Dates and numerical values must be validated before conversion.
7. Repeated job titles must not automatically be treated as duplicate jobs.
8. Exact duplicate detection must be distinguished from business level duplicate detection.
9. Original source data must remain unchanged in Bronze.
10. Sensitive recruiter contact information must not be exposed unnecessarily in public analytical outputs.

## 5. Current Limitations

1. The Rozee dataset does not contain company names.
2. The Pakistan Jobs dataset is small compared with the Rozee dataset.
3. The Pakistan Jobs salary and industry fields have no populated values.
4. The sources do not share a reliable job identifier.
5. The historical records do not establish a reliable stream of new jobs. Incremental ingestion will initially use controlled batch simulation.

## 6. Next Steps

1. Review and finalize the canonical field names.
2. Design the Bronze storage structure.
3. Implement source specific Python ingestion.
4. Capture ingestion metadata and logs.
5. Build validation and rerun handling.
