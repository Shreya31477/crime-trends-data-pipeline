"""V — Validate: sanity-check the raw data before transforming it.

Fail loudly (raise) when the data can't be trusted at all:
  * schema differs from what we expect
  * a date_time is not ISO 8601
  * a required column (date_time, id, type, location) has missing values
Fail softly for non-numeric IDs: they become NaN and are dropped in transform.
"""
from datetime import datetime

import pandas as pd

from prefect_compat import task

SCHEMA_COLS = ["date_time", "id", "type", "subtype", "location", "last_updated", "description"]
REQUIRED_COLS = ["date_time", "id", "type", "location"]


def check_valid_schema(df: pd.DataFrame) -> None:
    missing = set(SCHEMA_COLS) - set(df.columns)
    extra = set(df.columns) - set(SCHEMA_COLS)
    if missing:
        raise ValueError(f"Schema mismatch - missing columns: {sorted(missing)}")
    if extra:
        # New columns from the API shouldn't crash the pipeline; ignore them.
        print(f"Note: ignoring unexpected columns {sorted(extra)}")


def check_numeric_id(df: pd.DataFrame) -> pd.DataFrame:
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    bad = int(df["id"].isna().sum())
    if bad:
        print(f"Note: {bad} rows have non-numeric IDs and will be removed in transform")
    return df


def verify_datetime(df: pd.DataFrame) -> None:
    bad = []
    for value in df["date_time"].dropna().astype(str):
        try:
            datetime.fromisoformat(value)
        except ValueError:
            bad.append(value)
    if bad:
        raise ValueError(f"{len(bad)} date_time values are not ISO 8601, e.g. {bad[:3]}")


def check_missing_values(df: pd.DataFrame) -> None:
    for col in REQUIRED_COLS:
        n = int(df[col].isna().sum())
        if n:
            raise ValueError(f"{n} missing values in required column '{col}'")


@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("No data was extracted.")
    check_valid_schema(df)
    df = df[SCHEMA_COLS].copy()
    # Check for genuinely missing values BEFORE coercing IDs: coercion turns
    # non-numeric IDs into NaN on purpose (dropped later), which must not count as "missing".
    check_missing_values(df)
    verify_datetime(df)
    df = check_numeric_id(df)
    print(f"Validation passed for {len(df):,} rows")
    return df
