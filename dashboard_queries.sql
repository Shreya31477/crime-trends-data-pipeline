-- Ready-made queries for Metabase / pgAdmin dashboards (table: cpd_incidents)

-- 1. Incidents per month
SELECT date_trunc('month', date_time)::date AS month, COUNT(*) AS incidents
FROM cpd_incidents GROUP BY 1 ORDER BY 1;

-- 2. Incidents by hour of day
SELECT hour, COUNT(*) AS incidents FROM cpd_incidents GROUP BY 1 ORDER BY 1;

-- 3. Incidents by day of week
SELECT day_of_week, COUNT(*) AS incidents FROM cpd_incidents
GROUP BY 1 ORDER BY MIN(EXTRACT(ISODOW FROM date_time));

-- 4. Top incident types
SELECT type, COUNT(*) AS incidents FROM cpd_incidents GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- 5. Hotspot streets in the last 30 days
SELECT location, COUNT(*) AS incidents FROM cpd_incidents
WHERE date_time >= NOW() - INTERVAL '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- 6. Month-over-month change
WITH m AS (SELECT date_trunc('month', date_time)::date AS month, COUNT(*) AS n
           FROM cpd_incidents GROUP BY 1)
SELECT month, n, ROUND(100.0 * (n - LAG(n) OVER (ORDER BY month)) / LAG(n) OVER (ORDER BY month), 1) AS pct_change
FROM m ORDER BY month;
