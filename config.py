"""Shared settings for the pipeline, read from a .env file / environment variables."""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv not installed: fall back to plain environment variables
    pass

# --- Socrata (data source) ---
SOCRATA_DOMAIN = "data.cambridgema.gov"
DATASET_ID = "3gki-wyrb"  # Cambridge Police Daily Log
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # optional, avoids throttling

# --- PostgreSQL (destination) ---
# Defaults match the docker-compose setup (host port 5433 -> container 5432).
# In Google Colab (apt-installed Postgres) set POSTGRES_PORT=5432.
PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "cpd_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
}

TABLE = "cpd_incidents"
REPORT_DIR = os.getenv("REPORT_DIR", "reports")


def get_connection():
    import psycopg2
    return psycopg2.connect(**PG)
