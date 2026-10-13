# Clickstream Analytics & Bot Detection Pipeline

A learning project that builds a small data pipeline for a simulated e-commerce website: generate clickstream data with Python, save it as Parquet, analyse it with SQL in DuckDB, and report on it in Power BI.

**All data is synthetic.** I generated it myself and deliberately injected two problems (duplicate rows and a bot burst) so that I knew what the SQL queries should find.

## Pipeline

```text
Python (Pandas, NumPy)  ->  Parquet file  ->  DuckDB SQL queries  ->  Power BI report
 generate + inject          saved to disk     run on the DataFrame    DAX measures
 duplicates and a bot                         in the notebook
```

## Dataset

| Item | Value |
|---|---|
| Columns | `user_id`, `session_id`, `event_type`, `timestamp` |
| Event types | `view`, `click`, `scroll`, `cart_add` |
| Base events | 100,000, with users, session IDs and event types assigned randomly and timestamps evenly spaced across 2026 |
| Injected problem 1 | 5,000 exact duplicate rows (a copy of the first 5,000 events appended to the table) |
| Injected problem 2 | One bot, `user_9999` / `SESS_99999`: 250 click events with the same timestamp |
| Total rows | **105,250** (100,000 + 5,000 + 250) |

Events per type: `click` 26,429, `cart_add` 26,404, `view` 26,348, `scroll` 26,069.

## Repository files

| File | Purpose |
|---|---|
| `real-time-clickstream-analytics-fraud-processing.ipynb` | Data generation and SQL queries |
| `marged_table.parquet` | The generated dataset in Parquet format |
| `Real-Time Clickstream Analytics & Fraud Processing Engine.pbix` | Power BI report |

## SQL queries (DuckDB)

The queries run in the notebook against the pandas DataFrame `final_merged_table`. DuckDB reads the DataFrame directly, so there is no database to set up.

| # | Query | Question | Technique | Result |
|---|---|---|---|---|
| 1 | Bot burst detection | Which events are the 20th event within 10 seconds for the same user? | `LAG(timestamp, 19)` window function | 231 rows, all from `user_9999` |
| 2 | Consecutive-day streaks | How many days in a row was each user active? | Date minus `ROW_NUMBER()` (islands and gaps) | 99,116 rows. See *Known issues*: the original over-counts |
| 3 | Highest-volume sessions | Which sessions have the most events? | `GROUP BY`, `ORDER BY`, `LIMIT` | `SESS_99999` first with 250 events; the next highest has 3 |
| 4 | Event-type distribution | How many events of each type are there? | `GROUP BY` | Counts above; they add up to 105,250 |
| 5 | First view to first add-to-cart | How long between a session's first view and first add-to-cart? | `MIN(CASE ...)` per session | Returns rows, but some gaps are months long (see *Known issues*) |
| 6 | Click-to-scroll ratio | Which users have a ratio above 5 and more than 10 events? | Conditional `SUM` and `NULLIF` | 76 users |

Query 1 returns 231 rows because the bot made 250 clicks and the first 19 have no event 19 rows earlier to compare with.

### Query 1: bot burst detection

```sql
WITH main_table AS (
    SELECT user_id, session_id, event_type, timestamp,
           LAG(timestamp, 19) OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS prev_time
    FROM final_merged_table
)
SELECT user_id, session_id, event_type
FROM main_table
WHERE EPOCH(timestamp - prev_time) < 10;
```

`LAG(timestamp, 19)` looks at the event 19 rows earlier for the same user. If that event is less than 10 seconds before the current one, the user made 20 events in under 10 seconds.

### Query 2: consecutive-day streaks

Original query:

```sql
WITH main_table AS (
    SELECT user_id,
           CAST(timestamp AS DATE) - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp ASC) * INTERVAL '1 day') AS streak,
           timestamp
    FROM final_merged_table
)
SELECT user_id, COUNT(streak), MAX(timestamp), MIN(timestamp)
FROM main_table
GROUP BY user_id, streak;
```

Corrected version (one row per user per day first):

```sql
WITH days AS (
    SELECT DISTINCT user_id, CAST(timestamp AS DATE) AS activity_date
    FROM final_merged_table
),
ranked AS (
    SELECT user_id, activity_date,
           activity_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) * INTERVAL '1 day') AS streak_anchor
    FROM days
)
SELECT user_id, COUNT(*) AS streak_days,
       MIN(activity_date) AS start_date, MAX(activity_date) AS end_date
FROM ranked
GROUP BY user_id, streak_anchor
ORDER BY streak_days DESC, user_id;
```

On this dataset the corrected version gives a longest streak of 5 consecutive days (`user_4995`, 2026-12-11 to 2026-12-15).

### Query 3: highest-volume sessions

```sql
SELECT user_id, session_id, COUNT(*) AS count
FROM final_merged_table
GROUP BY user_id, session_id
ORDER BY COUNT(*) DESC
LIMIT 10;
```

### Query 4: event-type distribution

```sql
SELECT event_type, COUNT(*) AS count
FROM final_merged_table
GROUP BY event_type;
```

### Query 5: first view to first add-to-cart

```sql
WITH main_table AS (
    SELECT user_id, session_id,
           MIN(CASE WHEN event_type = 'view' THEN timestamp END) AS min_view,
           MIN(CASE WHEN event_type = 'cart_add' THEN timestamp END) AS min_cart_add
    FROM final_merged_table
    GROUP BY user_id, session_id
)
SELECT user_id, session_id, min_view, min_cart_add,
       EPOCH(min_cart_add - min_view) AS diff
FROM main_table
WHERE min_view < min_cart_add AND min_view IS NOT NULL AND min_cart_add IS NOT NULL;
```

### Query 6: click-to-scroll ratio

```sql
WITH new AS (
    SELECT user_id,
           CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS NUMERIC)
             / NULLIF(SUM(CASE WHEN event_type = 'scroll' THEN 1 ELSE 0 END), 0) AS click_scroll_ratio,
           COUNT(*) AS count
    FROM final_merged_table
    GROUP BY user_id
)
SELECT user_id, click_scroll_ratio
FROM new
WHERE click_scroll_ratio > 5.0 AND count > 10;
```

`NULLIF(..., 0)` turns a zero scroll count into NULL, so users with no scrolls do not cause a divide-by-zero error.

## Power BI

The report is built on the Parquet data and currently shows event counts by type (bar chart and table), an event-type slicer, a click-to-scroll ratio card (1.01 overall), and a table of events. The total row count in Power BI matches the source: 105,250. It does not yet show the detected bot.

## Known issues and limitations

- **Synthetic data, known answer.** I injected the bot myself, so Query 1 was tested against a case I already knew. On real traffic the thresholds (20 events, 10 seconds) would need tuning and a check for false positives. The query also only compares events within one `user_id`, so a bot that changed user IDs would not be caught.
- **Query 2 (original) assumes one event per user per day.** In this data 7,440 user-days have more than one event, and the extra rows shift the numbering. For example, one result row shows a "streak" of 2 events from 2026-01-08 to 2026-01-12, which is not consecutive. The corrected version removes duplicate days first.
- **Sessions are random, not realistic.** Session IDs are assigned at random across the whole year, so one "session" can last for months. For example, Query 5 shows a session whose first view is on 2026-07-31 and first add-to-cart is on 2026-11-16 (about 107 days). A better generator would give each session a short time window and several events.
- **Query 6 drops users with zero scrolls.** `NULLIF` avoids the divide-by-zero error, but the resulting NULL ratio is filtered out by the `WHERE` clause. This includes the bot (250 clicks, 0 scrolls), so this query cannot find it.
- **Batch, not real-time.** Despite the repository name, the pipeline processes a file in one run. It is not a streaming system.
- **No performance benchmark.** I have not measured Parquet against CSV, so the project makes no speed or compression claims.

## How to run

1. Install the packages: `pip install pandas numpy pyarrow duckdb`
2. Open the notebook and run all cells. It generates the data, saves the Parquet file and runs the queries.
3. Open the `.pbix` file in Power BI Desktop to view the report.

## Possible next steps

- Generate sessions with short time windows and several events each, so session-level queries make sense.
- Add the corrected streak query to the notebook.
- Add a bot-detection card and a top-sessions chart to the dashboard.
- Measure Parquet against CSV (file size and query time).
- Add simple automated checks for the row counts and injected anomalies.
