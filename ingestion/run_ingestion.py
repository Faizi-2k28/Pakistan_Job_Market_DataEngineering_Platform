
import json

from ingestion.rozee_ingestion import ingest_rozee
from ingestion.pakistan_jobs_ingestion import (
    ingest_pakistan_jobs,
)
from ingestion.jobspipe_ingestion import ingest_jobspipe


def main() -> None:
    results = []

    pipelines = [
        ("rozee", ingest_rozee),
        ("pakistan_jobs", ingest_pakistan_jobs),
        ("jobspipe", ingest_jobspipe),
    ]

    for source_name, ingestion_function in pipelines:
        try:
            result = ingestion_function()
            results.append(result)

        except Exception as exc:
            results.append({
                "source_name": source_name,
                "status": "failed",
                "error": str(exc),
            })

    print("\nCombined ingestion report:")
    print(json.dumps(results, indent=2))

    failed_runs = [
        result
        for result in results
        if result.get("status") == "failed"
    ]

    if failed_runs:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
