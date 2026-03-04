# Design Reflection

## What was the most critical issue you discovered?

The analytics table appending rows on every DAG run without clearing previous results. This is the kind of bug that is invisible until someone notices the numbers are wrong, and by then the damage is compounded across every run that occurred since deployment. A daily pipeline running for a week would produce spend figures 7x higher than reality. The fix itself is straightforward (delete before insert within a transaction), but the diagnostic difficulty is what makes it dangerous: the pipeline reports success, the table has data, and the aggregation query is logically correct. Nothing fails. The numbers just silently grow.

I consider this more critical than the SQL injection because the injection requires a compromised upstream API to exploit, while the idempotency bug corrupts data on every single scheduled run by design.

---

## What trade-offs did you make?

**Full reload vs. incremental processing.** I chose to delete and reload each layer on every run rather than implementing incremental/delta processing. This means the pipeline reprocesses all data every time, which is wasteful at scale but provides a strong correctness guarantee: the output is always a complete, consistent view of the current source data. Incremental processing would require tracking watermarks, handling late-arriving data, and managing merge logic, all of which add complexity that is not justified for a dataset of this size.

**Latest-tier-wins for customer tier changes.** CUST002 has two tier records (Platinum on Jan 1, Gold on Feb 15). I resolved this by always using the most recent tier for all of a customer's shipments, regardless of when the shipment occurred. An alternative would be to apply the tier that was active at the time of each shipment (a slowly changing dimension type 2 approach). I chose the simpler approach because the challenge specification asks for "Total Shipping Spend per Customer Tier per Month" without specifying historical tier tracking, and the SCD2 approach would require significantly more complex join logic and a temporal tier validity table.

**Filtering vs. quarantining bad data.** Records with negative costs, null customer IDs, and cancelled statuses are filtered out during transformation. An alternative would be to move them to a quarantine table for review. I chose filtering because the dataset is small and the bad records are clearly invalid, but in a production system I would add a quarantine table and alerting.

---

## Where could your solution still fail?

**API schema changes.** If the upstream API renames `shipping_cost` to `cost` or changes the response envelope structure, the extraction will fail. There is no schema validation on the API response beyond checking for the `data` key.

**Large CSV files.** The customer tiers extraction uses `pandas.read_csv()` which loads the entire file into memory. For a file with millions of rows, this would exhaust available memory. A chunked reader or COPY-based approach would be needed.

**Concurrent DAG runs.** While `max_active_runs=1` prevents concurrent scheduled runs, a manual trigger while a scheduled run is active could still cause race conditions. The `DELETE FROM` + `INSERT` pattern within a transaction mitigates this at the database level, but the extraction step could still produce inconsistent raw data if two runs overlap.

**Clock skew between containers.** The `loaded_at` timestamps use `CURRENT_TIMESTAMP` from PostgreSQL, which avoids cross-container clock issues, but if the system clock on the database server drifts, the `DISTINCT ON ... ORDER BY loaded_at DESC` logic could pick the wrong record.

---

## How would this system behave at 100x data volume?

At 100x the current volume (roughly 2,000 shipments and 600 tier records), the following bottlenecks would emerge:

**API extraction** would need pagination. Fetching 2,000 records in a single HTTP request is feasible, but the lack of pagination means the API must serialize the entire dataset into one response. At 100x, response times would degrade and timeouts would become likely. I would add offset/limit or cursor-based pagination to the extraction logic.

**Row-by-row INSERT loops** in the extraction scripts would become the primary bottleneck. Each INSERT is a separate round-trip to PostgreSQL. At 2,000 rows, this takes seconds; at 200,000 rows, it would take minutes. I would switch to `psycopg2.extras.execute_values()` for batch inserts or use PostgreSQL's `COPY` protocol for bulk loading.

**The full-reload strategy** (DELETE + INSERT) would cause increasingly long table locks. At high volume, I would switch to incremental processing with a staging-to-target merge (UPSERT) pattern, loading only new or changed records identified by a watermark column.

**The single-node Airflow executor** (LocalExecutor) would limit parallelism. At higher volumes, migrating to CeleryExecutor or KubernetesExecutor would allow extraction tasks to run on separate workers.

---

## What would you redesign with more time?

**Incremental extraction with watermarks.** Track the last successfully processed `shipment_date` or API cursor position, and only fetch new records on each run. This eliminates redundant processing and reduces load on both the API and the database.

**Slowly changing dimension type 2 for customer tiers.** Instead of picking the latest tier, maintain a history table with validity date ranges. Join shipments to the tier that was active on the shipment date. This produces more accurate historical analytics.

**Data quality framework.** Add a dedicated validation step between extract and transform that runs configurable rules (null checks, range checks, referential integrity checks) and writes violations to a quarantine table with alerting.

**Airflow Connections for credential management.** Instead of environment variables, use Airflow's built-in Connection objects to store database and API credentials. This provides encryption at rest, UI-based management, and audit logging of credential access.

**Monitoring and alerting.** Add row count assertions between pipeline stages (e.g., "enriched row count should be within 10% of raw row count"), integrate with a metrics system (Prometheus/StatsD), and configure Airflow email alerts on failure.

---

## Deep Dives

### Explain one complex function line-by-line

The `transform_shipment_data()` function in `scripts/transform_data.py`:

```python
def transform_shipment_data():
    logger.info("Starting data transformation")
```
Entry point. Logs the start of transformation using Airflow's task logger for integration with the Airflow log viewer.

```python
    with get_connection() as conn:
        with get_cursor(conn) as cur:
            cur.execute("BEGIN;")
```
Opens a database connection and cursor using context managers from `db.py`. The explicit `BEGIN` starts a transaction so that all following operations are atomic. If anything fails before `conn.commit()`, the context manager's exception handler calls `conn.rollback()`.

```python
            cur.execute("DELETE FROM staging.shipments_deduped;")
            cur.execute(
                """
                INSERT INTO staging.shipments_deduped ...
                SELECT DISTINCT ON (shipment_id)
                    shipment_id, customer_id, shipping_cost, shipment_date, status
                FROM raw.shipments
                WHERE shipment_id IS NOT NULL
                  AND shipping_cost > 0
                  AND status != 'cancelled'
                  AND customer_id IS NOT NULL
                ORDER BY shipment_id, loaded_at DESC
                """
            )
```
First, clears the deduplication target table. Then inserts from the raw layer with four filters applied simultaneously: removes records with null IDs, non-positive costs, cancelled status, and null customer references. `DISTINCT ON (shipment_id)` is a PostgreSQL extension that keeps only the first row per `shipment_id` group as determined by the `ORDER BY`. Since we order by `loaded_at DESC`, this picks the most recently loaded version of each shipment. This single query handles deduplication, data quality filtering, and conflict resolution in one pass.

```python
            cur.execute("DELETE FROM staging.customer_tiers_resolved;")
            cur.execute(
                """
                INSERT INTO staging.customer_tiers_resolved ...
                SELECT DISTINCT ON (customer_id)
                    customer_id, customer_name, tier, tier_updated_date
                FROM raw.customer_tiers
                WHERE customer_id IS NOT NULL
                ORDER BY customer_id, tier_updated_date DESC
                """
            )
```
Same pattern for customer tiers: one row per customer, keeping the most recent tier assignment. This resolves the CUST002 duplicate (Platinum Jan 1 vs Gold Feb 15) by selecting Gold.

```python
            cur.execute("DELETE FROM staging.shipments_enriched;")
            cur.execute(
                """
                INSERT INTO staging.shipments_enriched ...
                SELECT s.*, COALESCE(t.tier, 'Unknown') AS tier, t.customer_name
                FROM staging.shipments_deduped s
                LEFT JOIN staging.customer_tiers_resolved t ON s.customer_id = t.customer_id
                """
            )
```
Joins the clean shipments with the resolved tiers. LEFT JOIN ensures shipments for unknown customers (like CUST999) are preserved rather than dropped. `COALESCE(t.tier, 'Unknown')` assigns a default tier label to unmatched customers. At this point the data is clean, deduplicated, enriched, and ready for aggregation.

```python
            conn.commit()
```
Commits the entire transaction. All three staging tables are updated atomically. If any step failed, none of the changes would have been applied.

### Explain one SQL transformation step-by-step

The analytics aggregation query in `load_analytics.py`:

```sql
DELETE FROM analytics.shipping_spend_by_tier;

INSERT INTO analytics.shipping_spend_by_tier
    (tier, year_month, total_shipping_spend, shipment_count)
SELECT
    tier,
    TO_CHAR(shipment_date, 'YYYY-MM') AS year_month,
    SUM(shipping_cost)                 AS total_shipping_spend,
    COUNT(*)                           AS shipment_count
FROM staging.shipments_enriched
GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM')
ORDER BY year_month, tier
```

Step 1: `DELETE FROM analytics.shipping_spend_by_tier` clears all existing analytics rows. This is what makes the pipeline idempotent. Running it 100 times produces the same result as running it once.

Step 2: The `SELECT` reads from `shipments_enriched`, which at this point contains only valid, deduplicated, enriched records.

Step 3: `TO_CHAR(shipment_date, 'YYYY-MM')` extracts the year-month string from each shipment date. This is the time grain for aggregation.

Step 4: `GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM')` groups records by their tier label and month. Each group becomes one output row.

Step 5: `SUM(shipping_cost)` calculates the total spend for each tier-month combination. `COUNT(*)` gives the number of shipments in that group.

Step 6: The `INSERT INTO` places these aggregated results into the analytics table. The `ORDER BY` ensures deterministic output ordering for logging and debugging.

The `DELETE` and `INSERT` are wrapped in a single transaction. If the INSERT fails (e.g., a constraint violation), the DELETE is rolled back and the previous analytics data remains intact.

### Describe one alternative design you considered and rejected

**Materialized view instead of a physical analytics table.** PostgreSQL supports `CREATE MATERIALIZED VIEW` which could replace the entire load step with:

```sql
CREATE MATERIALIZED VIEW analytics.shipping_spend_by_tier AS
SELECT tier, TO_CHAR(shipment_date, 'YYYY-MM') AS year_month, ...
FROM staging.shipments_enriched
GROUP BY ...;
```

Refreshing it would be a single `REFRESH MATERIALIZED VIEW` call. This approach is appealing because it eliminates the manual delete-insert pattern and guarantees the view definition stays in sync with the query.

I rejected it for three reasons. First, `REFRESH MATERIALIZED VIEW` takes an exclusive lock on the view for the duration of the refresh, blocking all reads. The `CONCURRENTLY` option avoids this but requires a unique index and is slower. Second, materialized views cannot have additional columns like `calculated_at` that track when the aggregation was computed, which is useful for auditing. Third, the explicit delete-insert pattern gives us full control over error handling and logging within the Python code, including the ability to log row counts and individual tier-month breakdowns before committing.
