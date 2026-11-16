"""Full pipeline: Extract -> Validate -> Transform -> Load -> Monitor.

    python etl_pipeline.py            # run once
    python etl_pipeline.py --serve    # register a Prefect deployment that runs daily at midnight
"""
import sys

from extract import extract_data
from validate import validate_data
from transform import transform_data
from load import load_into_postgres
from monitor import monitor_trends
from prefect_compat import flow, HAS_PREFECT


@flow(name="cpd_incident_etl", log_prints=True)  # shows as 'cpd_incident_etl' in the Prefect UI
def etl():
    print("Extracting data...")
    raw = extract_data()

    print("Performing data quality checks...")
    validated = validate_data(raw)

    print("Performing data transformations...")
    transformed = transform_data(validated)

    print("Importing data into Postgres...")
    load_into_postgres(transformed)

    print("Analysing crime trends...")
    monitor_trends()

    print("ETL complete!")


if __name__ == "__main__":
    if "--serve" in sys.argv:
        if not HAS_PREFECT:
            sys.exit("Install Prefect (pip install prefect) to schedule the pipeline.")
        # CPD data updates daily, so run every day at midnight
        etl.serve(name="cpd-pipeline-deployment", cron="0 0 * * *")
    else:
        etl()
