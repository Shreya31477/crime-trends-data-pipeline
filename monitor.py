"""Monitor: query PostgreSQL for crime trends, flag unusual spikes, save charts + a report.

Outputs (in REPORT_DIR, default ./reports):
  monthly_trend.png, by_hour.png, top_types.png, top_locations.png, trend_report.md
"""
import os
from datetime import datetime

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from config import TABLE, REPORT_DIR, get_connection
from prefect_compat import task

# ---------- SQL used for monitoring (also handy to paste into Metabase / pgAdmin) ----------
QUERIES = {
    "monthly": f"""
        SELECT date_trunc('month', date_time)::date AS month, COUNT(*) AS incidents
        FROM {TABLE} GROUP BY 1 ORDER BY 1""",
    "monthly_by_type": f"""
        SELECT date_trunc('month', date_time)::date AS month, type, COUNT(*) AS incidents
        FROM {TABLE} GROUP BY 1, 2 ORDER BY 1, 2""",
    "by_hour": f"""
        SELECT hour, COUNT(*) AS incidents FROM {TABLE} GROUP BY 1 ORDER BY 1""",
    "top_types": f"""
        SELECT type, COUNT(*) AS incidents FROM {TABLE}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
    "top_locations_30d": f"""
        SELECT location, COUNT(*) AS incidents FROM {TABLE}
        WHERE date_time >= (SELECT MAX(date_time) FROM {TABLE}) - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10""",
}

Z_THRESHOLD = 2.0     # flag a month that is > 2 standard deviations above its baseline
BASELINE_MONTHS = 12  # compare against the previous 12 months
MIN_BASELINE = 6      # need at least this many months of history to judge a spike


def run_query(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def complete_months(monthly: pd.DataFrame, last_ts) -> pd.DataFrame:
    """Drop the current month if it's still in progress, so it isn't compared unfairly."""
    monthly = monthly.copy()
    monthly["month"] = pd.to_datetime(monthly["month"])
    last_ts = pd.Timestamp(last_ts)
    month_end = (last_ts + pd.offsets.MonthEnd(0)).normalize()
    if last_ts.normalize() < month_end:
        monthly = monthly[monthly["month"] < last_ts.to_period("M").to_timestamp()]
    return monthly


def detect_spikes(monthly_by_type: pd.DataFrame) -> pd.DataFrame:
    """For each incident type, z-score of the latest complete month vs the prior 12 months."""
    pivot = (monthly_by_type.pivot_table(index="month", columns="type",
                                         values="incidents", aggfunc="sum")
                            .sort_index().fillna(0))
    if len(pivot) < MIN_BASELINE + 1:
        return pd.DataFrame(columns=["type", "latest", "baseline_mean", "z_score"])
    latest = pivot.iloc[-1]
    base = pivot.iloc[-(BASELINE_MONTHS + 1):-1]
    mean, std = base.mean(), base.std(ddof=0).replace(0, float("nan"))
    z = (latest - mean) / std
    res = pd.DataFrame({"type": pivot.columns, "latest": latest.values,
                        "baseline_mean": mean.round(1).values, "z_score": z.round(2).values})
    return (res[(res["z_score"] > Z_THRESHOLD) & (res["latest"] >= 5)]
              .sort_values("z_score", ascending=False))


def _bar(df, x, y, title, path, horizontal=False):
    fig, ax = plt.subplots(figsize=(9, 5))
    if horizontal:
        d = df.iloc[::-1]
        ax.barh(d[x].astype(str), d[y], color="#3b6ea5")
    else:
        ax.bar(df[x].astype(str), df[y], color="#3b6ea5")
    ax.set_title(title)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


@task
def monitor_trends() -> dict:
    os.makedirs(REPORT_DIR, exist_ok=True)
    conn = get_connection()
    try:
        data = {name: run_query(conn, sql) for name, sql in QUERIES.items()}
        last_ts = run_query(conn, f"SELECT MAX(date_time) AS m FROM {TABLE}")["m"].iloc[0]
    finally:
        conn.close()

    monthly = complete_months(data["monthly"], last_ts)
    mbt = data["monthly_by_type"].copy()
    mbt["month"] = pd.to_datetime(mbt["month"])
    mbt = mbt[mbt["month"].isin(monthly["month"])]

    # month-over-month change + 3-month rolling average
    monthly["incidents"] = monthly["incidents"].astype(int)
    monthly["mom_change_pct"] = (monthly["incidents"].pct_change() * 100).round(1)
    monthly["rolling_3m"] = monthly["incidents"].rolling(3).mean().round(1)
    spikes = detect_spikes(mbt)

    # ---- charts ----
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(monthly["month"], monthly["incidents"], marker="o", ms=3, label="Monthly incidents")
    ax.plot(monthly["month"], monthly["rolling_3m"], lw=2, label="3-month average")
    ax.set_title("Reported incidents per month (Cambridge PD)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(REPORT_DIR, "monthly_trend.png"), dpi=120)
    plt.close(fig)
    _bar(data["by_hour"], "hour", "incidents", "Incidents by hour of day",
         os.path.join(REPORT_DIR, "by_hour.png"))
    _bar(data["top_types"], "type", "incidents", "Top 10 incident types (all time)",
         os.path.join(REPORT_DIR, "top_types.png"), horizontal=True)
    _bar(data["top_locations_30d"], "location", "incidents", "Top 10 locations (last 30 days)",
         os.path.join(REPORT_DIR, "top_locations.png"), horizontal=True)

    # ---- markdown report ----
    lines = [f"# Crime trend report — generated {datetime.now():%Y-%m-%d %H:%M}", "",
             f"Data through: {pd.Timestamp(last_ts):%Y-%m-%d %H:%M}", ""]
    if len(monthly):
        last = monthly.iloc[-1]
        lines += ["## Latest complete month",
                  f"- {last['month']:%B %Y}: **{last['incidents']:,}** incidents "
                  f"({last['mom_change_pct']:+.1f}% vs previous month)"
                  if pd.notna(last["mom_change_pct"]) else
                  f"- {last['month']:%B %Y}: **{last['incidents']:,}** incidents", ""]
    lines += ["## Spike alerts (z-score > %.1f vs previous %d months)" % (Z_THRESHOLD, BASELINE_MONTHS)]
    if spikes.empty:
        lines.append("- No unusual spikes detected.")
    else:
        for r in spikes.itertuples():
            lines.append(f"- **{r.type}**: {int(r.latest)} incidents "
                         f"(usual ≈ {r.baseline_mean}, z = {r.z_score})")
    peak_hour = data["by_hour"].sort_values("incidents").iloc[-1]["hour"] if len(data["by_hour"]) else None
    lines += ["", "## Other observations",
              f"- Busiest hour of the day: {int(peak_hour):02d}:00" if peak_hour is not None else "",
              "- Most common types: " + ", ".join(data["top_types"]["type"].head(3).astype(str)),
              "", "## Last 12 months", "",
              monthly.tail(12).assign(month=lambda d: d["month"].dt.strftime("%Y-%m"))
                     .to_markdown(index=False) if len(monthly) else "(no data)"]
    report_path = os.path.join(REPORT_DIR, "trend_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    for alert in spikes.itertuples():
        print(f"ALERT: spike in {alert.type} ({int(alert.latest)} vs ~{alert.baseline_mean})")
    print(f"Report written to {report_path}")
    return {"monthly": monthly, "spikes": spikes, **data}
