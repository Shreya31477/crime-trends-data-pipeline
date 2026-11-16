"""T — Transform: clean the validated data and add time columns for trend analysis."""
import pandas as pd

from prefect_compat import task


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows whose id was non-numeric (marked NaN during validation)."""
    df = df.dropna(subset=["id"]).copy()
    df["id"] = df["id"].astype("int64")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """One row per id; if an incident appears twice keep the most recently updated version."""
    return (df.sort_values("last_updated", ascending=False, na_position="last")
              .drop_duplicates(subset=["id"], keep="first"))


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["type", "subtype", "location", "description"]:
        df[col] = df[col].astype("string").str.strip()
    df["type"] = df["type"].str.upper()
    return df


def split_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df["date_time"] = pd.to_datetime(df["date_time"])
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    dt = df["date_time"].dt
    df["year"], df["month"], df["day"] = dt.year, dt.month, dt.day
    df["hour"], df["minute"], df["second"] = dt.hour, dt.minute, dt.second
    df["day_of_week"] = dt.day_name()
    return df


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = remove_invalid_rows(df)
    df = remove_duplicates(df)
    df = clean_text(df)
    df = split_datetime(df)
    df = df.sort_values("date_time").reset_index(drop=True)
    print(f"Transformed: {before:,} -> {len(df):,} rows")
    return df
