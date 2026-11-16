"""E — Extract: download the Cambridge Police Daily Log via the Socrata Open Data API."""
import pandas as pd

from config import SOCRATA_DOMAIN, DATASET_ID, SOCRATA_APP_TOKEN
from prefect_compat import task


@task(retries=3, retry_delay_seconds=[10, 10, 10])  # retry the API request if it fails
def extract_data() -> pd.DataFrame:
    """Return every incident in the dataset as a pandas DataFrame."""
    from sodapy import Socrata

    if not SOCRATA_APP_TOKEN:
        print("Warning: SOCRATA_APP_TOKEN not set - requests may be throttled.")

    # timeout raised from the 10 s default; fetching everything can take longer
    client = Socrata(SOCRATA_DOMAIN, SOCRATA_APP_TOKEN, timeout=30)
    try:
        results = client.get_all(DATASET_ID)  # paginates over all records
        df = pd.DataFrame.from_records(results)
    finally:
        client.close()

    print(f"Extracted {len(df):,} rows")
    return df
