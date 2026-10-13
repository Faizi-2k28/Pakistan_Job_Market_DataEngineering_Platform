
# Pakistan Job Market Data Engineering Platform

## 1. Project Objective

Build an end to end data platform that ingests job market data from multiple sources, preserves raw records, processes the data using Apache Spark, and creates analytical datasets for reporting.

## 2. Data Sources

### Source A: Rozee Jobs 2024

File: RozeePK_Jobs.csv

Original rows: 1059

Columns: 12

Completely empty rows: 25

Exact duplicate occurrences among nonempty rows: 4

Important fields:

Title, Salary, Job Type, Job Location, Functional Area, Career Level, Apply Before, Minimum Experience, Minimum Education, Gender, Age, Skills.

### Source B: Pakistan Jobs

File: pakistan_jobs.csv

Original rows: 31

Columns: 11

Completely empty rows: 0

Exact duplicate rows: 0

Important fields:

jobTitle, companyName, postingDate, salaryRange, industry, recruiterPhone, location, jobType, skills, recruiterEmail, url.

## 3. Data Quality Findings

### Rozee

1. Salary is missing in 29.65 percent of rows.
2. Minimum Experience is missing in 29.46 percent of rows.
3. Age is missing in 84.14 percent of rows.
4. Functional Area and Career Level are each missing in 15.39 percent of rows.
5. Twenty five rows are completely empty.
6. Skills and locations require further inspection and standardization.
7. Salary values require parsing before numerical analysis.

### Pakistan Jobs

1. Salary and industry are completely empty.
2. Recruiter phone is missing in 96.77 percent of rows.
3. Location, job type and skills each have one missing value.
4. Skills are represented as text that can resemble a list.
5. The URL field contains structured information represented as text.
6. Recruiter contact details must be protected in the public repository.

## 4. Preliminary Schema Mapping

Rozee Title maps to job_title.

Pakistan Jobs jobTitle maps to job_title.

Rozee Job Location maps to location.

Pakistan Jobs location maps to location.

Rozee Job Type maps to job_type.

Pakistan Jobs jobType maps to job_type.

Rozee Skills maps to skills_raw.

Pakistan Jobs skills maps to skills_raw.

Pakistan Jobs companyName maps to company_name.

Rozee Salary maps to salary_raw.

Pakistan Jobs salaryRange maps to salary_raw, although its current values are entirely missing.

Rozee Functional Area maps to functional_area.

Pakistan Jobs industry remains a separate industry field. It must not be confused with functional_area.

Rozee Career Level maps to career_level.

Rozee Minimum Experience maps to experience_requirement.

Rozee Minimum Education maps to education_requirement.

Rozee Gender maps to gender_requirement.

Rozee Age maps to age_requirement.

Pakistan Jobs postingDate maps to posted_at.

Rozee Apply Before maps to application_deadline.

Pakistan Jobs url maps to job_url.

## 5. Important Design Decisions

1. Preserve original source files.
2. Do not clean or modify data during initial ingestion.
3. Store source identity and ingestion metadata.
4. Standardize the schemas during the Silver transformation stage.
5. Do not assume that repeated job titles represent duplicate jobs.
6. Investigate empty rows and duplicate records before applying cleaning rules.
7. Protect recruiter contact information and exclude unnecessary contact details from public analytical outputs.

## 6. Next Steps

1. Complete source profiling.
2. Finalize the unified job data contract.
3. Design the Bronze storage structure.
4. Implement Python ingestion for each source.
5. Add validation, logging and ingestion metadata.
