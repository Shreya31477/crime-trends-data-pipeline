# Data Pipeline to Monitor Local Crime Trends (PAI102)

ETL pipeline for the Cambridge (MA) Police Daily Log:

```
Socrata API ──► extract ──► validate ──► transform ──► load (PostgreSQL) ──► monitor (charts + spike alerts)
                                                              │
                                                              └──► Metabase dashboard
```

| File | Step |
|---|---|
| `extract.py` | Download all incidents from the Socrata API (dataset `3gki-wyrb`) |
| `validate.py` | Schema, ISO 8601 dates, required fields, numeric IDs |
| `transform.py` | Drop bad IDs, de-duplicate, tidy text, add year/month/day/hour/day_of_week |
| `load.py` | Create `cpd_incidents` and **upsert** rows (safe to run every day) |
| `monitor.py` | Monthly trend, month-over-month %, 3-month average, spike detection per type, hotspots → `reports/` |
| `etl_pipeline.py` | Prefect flow tying it together; `--serve` schedules it daily at midnight |
| `docker-compose.yml` | Postgres + pgAdmin + Metabase |
| `dashboard_queries.sql` | Queries to paste into Metabase |

## Run locally (Docker)

```bash
pip install -r requirements.txt
cp .env.example .env          # then edit the values
docker compose up -d          # Postgres :5433, pgAdmin :8081, Metabase :3000
python etl_pipeline.py        # run once
python etl_pipeline.py --serve  # optional: run daily via Prefect
```

Output: `reports/trend_report.md` plus `monthly_trend.png`, `by_hour.png`, `top_types.png`, `top_locations.png`.

**Metabase:** open http://localhost:3000, add a PostgreSQL database with host `postgres_cpd`,
port `5432`, database `cpd_db`, and your `POSTGRES_USER` / `POSTGRES_PASSWORD`.

## Run in Google Colab

Docker doesn't run inside Colab, so use the notebook, which installs Postgres directly
(port 5432) and runs every step cell by cell.

## Changes from the tutorial code

- **Validation bug fixed:** the missing-value check ran after non-numeric IDs were turned into NaN, so one bad ID crashed the pipeline instead of being dropped.
- **Load no longer uses `to_sql(if_exists='replace')`**, which dropped the table (and its primary key) every run and wrote an extra index column. It now upserts on `id`.
- Removed the nested-quote f-string in the DB URL (a syntax error before Python 3.12).
- Duplicates keep the most recently updated record; `last_updated` is stored as a timestamp.
- Compose healthcheck `interval: 15` → `15s` (a unit is required).
- Added the `monitor.py` trend-analysis step.
