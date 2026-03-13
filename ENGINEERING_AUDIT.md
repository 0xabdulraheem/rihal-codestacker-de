# Engineering Audit

## Issue 1: Analytics Table Appends on Every Run (No Idempotency)

- **Severity:** High
- **Description:** The `load_analytics_data` function uses a bare `INSERT INTO analytics.shipping_spend_by_tier` without clearing previous results. Every DAG run appends a full duplicate set of aggregated rows, causing totals to double, triple, and so on with each execution.
- **Impact:** All downstream reports and dashboards consuming this table would show inflated spend numbers. In a production daily schedule, the data would be meaningless within 48 hours. Any business decision based on these numbers would be wrong.
- **Mitigation:** Wrapped the analytics load in a transaction that issues `DELETE FROM analytics.shipping_spend_by_tier` before the `INSERT ... SELECT` aggregation. The entire operation is atomic: if the insert fails, the delete is rolled back and the previous correct data remains intact. Added a composite primary key `(tier, year_month)` on the analytics table as an additional safety net against duplicate rows.

---

## Issue 2: SQL Injection in Shipment Extraction

- **Severity:** High
- **Description:** `extract_shipments.py` constructed SQL statements using Python f-strings with values received directly from the external API. Any shipment record containing a single quote or malicious payload in fields like `shipment_id`, `customer_id`, or `status` would break the query or allow arbitrary SQL execution.
- **Impact:** An attacker controlling the upstream API (or a man-in-the-middle) could drop tables, exfiltrate data, or escalate privileges on the PostgreSQL instance. Even without malicious intent, a customer name containing an apostrophe would crash the pipeline.
- **Mitigation:** Replaced all f-string SQL construction with parameterized queries using `%s` placeholders and tuple parameters. psycopg2 handles escaping and type conversion safely.

---

## Issue 3: Hardcoded Database Credentials

- **Severity:** High
- **Description:** Every Python script contained `host="postgres", database="airflow", user="airflow", password="airflow"` in plain text. The docker-compose file also had credentials inline.
- **Impact:** Credentials committed to version control are visible to anyone with repository access. In a real deployment, this violates compliance requirements (SOC2, GDPR) and makes credential rotation impossible without code changes.
- **Mitigation:** Created a shared `scripts/db.py` module that reads all connection parameters from environment variables (`PIPELINE_DB_HOST`, `PIPELINE_DB_USER`, etc.) with sensible defaults for local development. The docker-compose file uses `${POSTGRES_PASSWORD:-airflow}` syntax so that production deployments can override via a `.env` file that is gitignored.

---

## Issue 4: DROP TABLE Destroys Data Without Safety

- **Severity:** High
- **Description:** Both extraction scripts used `DROP TABLE IF EXISTS staging.X` followed by `CREATE TABLE` and inserts. If the pipeline failed mid-execution (between the DROP and the commit), the staging data would be permanently lost with no way to recover.
- **Impact:** A network blip during API extraction would leave the staging schema empty. The transform step would then produce zero rows, and the analytics table would be wiped clean by the idempotent load, resulting in complete data loss until the next successful full run.
- **Mitigation:** Replaced the DROP/CREATE pattern with pre-created persistent tables (defined in `init.sql`) and `DELETE FROM` within explicit transactions. If any step fails, the transaction rolls back and the previous data remains untouched.

---

## Issue 5: Port Conflict Between API and Airflow

- **Severity:** High
- **Description:** The docker-compose file mapped both the mock API (`8080:8000`) and the Airflow webserver (`8080:8080`) to host port 8080. Only one service can bind to a host port, so whichever starts second would fail or shadow the other.
- **Impact:** Either the Airflow UI or the API would be inaccessible from the host, making debugging and monitoring impossible. The pipeline itself uses the Docker internal network so it would still function, but operators would have no visibility.
- **Mitigation:** Changed the API port mapping to `8000:8000` and kept Airflow at `8080:8080`. Each service now has a dedicated, non-conflicting host port.

---

## Issue 6: Airflow Configuration Exposed

- **Severity:** High
- **Description:** `AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True` in docker-compose exposes the full Airflow configuration (including database connection strings, secret keys, and internal settings) through the web UI to any authenticated user.
- **Impact:** Any user with Airflow UI access (which uses default admin/admin credentials) can see all database passwords, API keys, and internal configuration. This is an information disclosure vulnerability.
- **Mitigation:** Set `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` to `False`.

---

## Issue 7: No API Error Handling or Retry Logic

- **Severity:** High
- **Description:** `extract_shipments.py` called `requests.get()` without a timeout, without checking the HTTP status code, and without any retry mechanism. The mock API simulates 500 errors every 10th request and 5-second delays every 7th request.
- **Impact:** A single API error would crash the entire DAG run. The 5-second delay with no timeout could cause the request to hang indefinitely in production if the upstream service is unresponsive.
- **Mitigation:** Added `fetch_shipments_with_retry()` with exponential backoff (3 attempts), a 30-second timeout per request, `response.raise_for_status()` to catch HTTP errors, and proper exception handling that logs each failed attempt before retrying.

---

## Issue 8: Duplicate Shipment Records Not Handled

- **Severity:** High
- **Description:** The API returns SHP002 twice with different shipping costs (45.00 and 47.00). The original pipeline inserted both records, which inflated the aggregated spend for that customer.
- **Impact:** Duplicate source records lead to double-counted revenue/spend in analytics. Business reports would overstate shipping costs for affected customers.
- **Mitigation:** The transform step uses `DISTINCT ON (shipment_id)` ordered by `loaded_at DESC` to keep only the most recent version of each shipment. This handles both exact duplicates and records that were updated upstream.

---

## Issue 9: Customer Tier Changes Create Duplicate Joins

- **Severity:** High
- **Description:** The CSV contains two entries for CUST002: Platinum (2024-01-01) and Gold (2024-02-15). The original LEFT JOIN on `customer_id` produced a cartesian product, doubling every CUST002 shipment in the output.
- **Impact:** Every shipment for a customer with multiple tier records would be counted once per tier record, inflating both spend and count in the analytics table.
- **Mitigation:** Added a `customer_tiers_resolved` staging table that uses `DISTINCT ON (customer_id)` ordered by `tier_updated_date DESC` to select only the most recent tier assignment for each customer before joining.

---

## Issue 10: Negative and Zero Shipping Costs Included

- **Severity:** Medium
- **Description:** SHP012 has a shipping cost of -5.00 and SHP013 has 0.00. These were included in the analytics aggregation without question.
- **Impact:** Negative costs reduce the total spend aggregation, producing misleading numbers. Zero-cost shipments inflate the shipment count without contributing to spend, skewing average calculations.
- **Mitigation:** The transform step filters out records where `shipping_cost <= 0`. A `CHECK (shipping_cost > 0)` constraint on the `shipments_deduped` table provides a database-level safety net.

---

## Issue 11: Null Customer IDs Included

- **Severity:** Medium
- **Description:** SHP014 has a null `customer_id`. The LEFT JOIN would assign it to the "Unknown" tier, but a null customer ID is likely bad source data that should not flow into analytics.
- **Impact:** Shipments with no customer attribution cannot be meaningfully categorized. Including them pollutes the "Unknown" tier bucket and masks data quality problems upstream.
- **Mitigation:** The transform step filters out records where `customer_id IS NULL`.

---

## Issue 12: Cancelled Shipments Counted in Spend

- **Severity:** Medium
- **Description:** SHP017 has status "cancelled" but was included in the spend aggregation.
- **Impact:** Cancelled shipments represent costs that were never incurred. Including them overstates actual shipping spend.
- **Mitigation:** The transform step filters out records where `status = 'cancelled'`.

---

## Issue 13: No Raw Data Layer

- **Severity:** Medium
- **Description:** The original pipeline loaded API data directly into staging tables, with no preservation of the raw input. If a transformation bug corrupted the data, there was no way to reprocess from the original source without re-calling the API.
- **Impact:** No auditability, no ability to replay transformations, no data lineage. If the API changes its schema or becomes unavailable, historical raw data is lost.
- **Mitigation:** Introduced a `raw` schema with `raw.shipments` and `raw.customer_tiers` tables. Extract steps load into raw, transform steps read from raw and write to staging. Raw data is preserved across transformation changes.

---

## Issue 14: No Transaction Management

- **Severity:** Medium
- **Description:** The original scripts issued individual SQL statements without explicit transaction boundaries. A failure mid-way through insertion would leave tables in a partially loaded state.
- **Impact:** Partial data loads produce incorrect aggregations. The pipeline has no way to distinguish between a complete load and a partial one.
- **Mitigation:** All database operations are now wrapped in explicit `BEGIN`/`COMMIT` blocks using context managers. The `get_connection()` context manager issues a `ROLLBACK` on any unhandled exception.

---

## Issue 15: No Connection Management

- **Severity:** Medium
- **Description:** Each script created its own `psycopg2.connect()` call with duplicated parameters, and connections were closed manually without protection against exceptions leaving connections open.
- **Impact:** Connection leaks under error conditions. Duplicated connection logic makes maintenance error-prone.
- **Mitigation:** Centralized all connection management in `scripts/db.py` with context managers (`get_connection`, `get_cursor`) that guarantee cleanup regardless of success or failure.

---

## Issue 16: Print Statements Instead of Structured Logging

- **Severity:** Medium
- **Description:** All scripts used `print()` for output. Airflow captures stdout, but print statements lack log levels, timestamps, and structured formatting.
- **Impact:** Difficult to filter important messages from noise. No way to set log levels per environment. Missing context for debugging production issues.
- **Mitigation:** Replaced all `print()` calls with Python's `logging` module using the `airflow.task` logger, which integrates with Airflow's log management and supports level-based filtering.

---

## Issue 17: Unused Import

- **Severity:** Low
- **Description:** The DAG file imported `BashOperator` but never used it.
- **Impact:** No functional impact, but unused imports indicate careless code review and add unnecessary dependencies.
- **Mitigation:** Removed the unused import.

---

## Issue 18: No Input Validation on CSV

- **Severity:** Medium
- **Description:** `extract_customer_tiers.py` loaded the CSV without checking if the file exists or if expected columns are present. A missing or malformed CSV would produce a cryptic pandas error.
- **Impact:** Unclear error messages slow down incident response. A renamed column in the source file would silently produce null values rather than failing fast.
- **Mitigation:** Added explicit file existence check and column validation before processing. Missing required columns raise a descriptive `ValueError`.

---

## Issue 19: No Maximum Active DAG Runs

- **Severity:** Medium
- **Description:** The DAG had no `max_active_runs` setting, allowing multiple concurrent executions of the same pipeline. With the non-idempotent design, concurrent runs would corrupt data even more severely.
- **Impact:** Race conditions between concurrent runs could cause table locks, deadlocks, or interleaved partial writes.
- **Mitigation:** Set `max_active_runs=1` on the DAG to ensure only one execution runs at a time.

---

## Issue 20: Insufficient Retry Configuration

- **Severity:** Low
- **Description:** The DAG had `retries: 1` with a 1-minute delay. Given the API's simulated intermittent failures, a single retry is often insufficient.
- **Impact:** Transient API failures would cause the DAG to fail unnecessarily, requiring manual intervention to restart.
- **Mitigation:** Increased retries to 3 with a 2-minute delay, and added a 15-minute `execution_timeout` to prevent indefinite hangs.

---

## Issue 21: No Schema Constraints or Primary Keys

- **Severity:** Medium
- **Description:** The original `init.sql` created only empty schemas with no table definitions. Tables were created dynamically by the pipeline scripts with no constraints, primary keys, or data types beyond basic VARCHAR/DECIMAL.
- **Impact:** No database-level protection against duplicate records, null values in critical columns, or invalid data types. The database cannot enforce data integrity independently of the application code.
- **Mitigation:** Pre-defined all tables in `init.sql` with appropriate constraints: `PRIMARY KEY` on `shipment_id` in staging tables, `NOT NULL` on required fields, `CHECK (shipping_cost > 0)` on the deduped table, and a composite primary key `(tier, year_month)` on the analytics table.

---

## Issue 22: Unknown Customer ID Handling

- **Severity:** Low
- **Description:** SHP011 references CUST999, which does not exist in the customer tiers CSV. The LEFT JOIN correctly produces a NULL tier, but COALESCE maps it to "Unknown" without any logging or flagging.
- **Impact:** Unknown customers are silently absorbed into analytics. There is no mechanism to alert data stewards that new customers are appearing in shipment data without corresponding tier assignments.
- **Mitigation:** The transform step preserves "Unknown" tier mapping via COALESCE for completeness, and the enriched table's NOT NULL constraint on `tier` ensures no nulls slip through. The logging output includes record counts at each stage, making it visible when records fall into the "Unknown" bucket.

---

## Issue 23: No Pipeline Observability or Metrics

- **Severity:** Medium
- **Description:** The original pipeline had no mechanism to track execution statistics, row counts per stage, processing duration, or data quality over time. Operators had no way to detect gradual data drift or degraded performance without manually inspecting logs.
- **Impact:** Silent failures (e.g., the API returning fewer records than expected) would go unnoticed. There was no historical record of pipeline health to support capacity planning or incident investigation.
- **Mitigation:** Added `analytics.pipeline_metrics` table that records rows processed, rows rejected, duration, and status for each pipeline stage on every run. Added `analytics.data_quality_log` table that records pass/fail results for nine automated quality checks. Created `scripts/metrics.py` with a `timed_stage` context manager that automatically captures timing and row counts. Created `scripts/validate_data.py` as a dedicated DAG step that runs after the analytics load.

---

## Issue 24: No Automated Data Quality Checks

- **Severity:** Medium
- **Description:** The original pipeline had no post-load validation. There was no automated way to verify that the output was correct, consistent, or complete after each run.
- **Impact:** Data quality problems introduced by upstream changes (e.g., the API adding new fields, the CSV format changing) would silently propagate to the analytics layer and into downstream reports.
- **Mitigation:** Added a `validate_data_quality` task as the final step in the DAG. It runs ten checks: raw table not empty, deduped count less than or equal to raw count, enriched count matches deduped count, analytics not empty, no negative costs in staging, no duplicate shipment IDs, no null tiers in enriched data, no non-positive spend in analytics, sum reconciliation between staging and analytics layers, and quarantine records logged. Each check result is persisted to `analytics.data_quality_log`.

---

## Issue 25: Rejected Records Silently Discarded

- **Severity:** Medium
- **Description:** The transform step filtered out records with negative costs, null customer IDs, cancelled status, and duplicates, but provided no mechanism to inspect what was rejected or why. In a production environment, data stewards have no visibility into data loss.
- **Impact:** Upstream data quality problems go unnoticed. A sudden spike in rejections (e.g., the API returning mostly null customer IDs) would silently reduce the analytics output without any alert. Compliance audits cannot trace what happened to specific records.
- **Mitigation:** Added a `staging.quarantine` table that stores every rejected record as a JSONB payload alongside a categorical `rejection_reason` (`null_shipment_id`, `null_customer_id`, `invalid_shipping_cost`, `cancelled_shipment`, `duplicate_shipment_id`). The quarantine is cleared and repopulated on each run to stay idempotent. A Quarantine tab in the Streamlit dashboard visualizes rejection breakdowns. The `validate_data_quality` step now includes a quarantine audit check.

---

## Issue 26: No API Response Schema Validation

- **Severity:** Medium
- **Description:** The extraction script trusted the upstream API response structure completely. If the API changed its response envelope (e.g., renaming `data` to `results`) or dropped fields from shipment records, the pipeline would fail with cryptic `KeyError` or silently produce null columns.
- **Impact:** Schema drift in upstream APIs is a common production failure mode. Without validation, the pipeline cannot distinguish between "the API returned bad data" and "our extraction code is broken," making incident response slower.
- **Mitigation:** Added `validate_api_response()` that verifies the response is a dict, contains a `data` key, that `data` is a list, and that every record contains all five required fields (`shipment_id`, `customer_id`, `shipping_cost`, `shipment_date`, `status`). Validation runs before any database writes, failing fast with a descriptive error message that names exactly which records and fields are missing.

---

## Issue 27: No Database Indexes on Frequently Queried Columns

- **Severity:** Low
- **Description:** The raw layer tables had no indexes. The `DISTINCT ON (shipment_id) ORDER BY loaded_at DESC` query in the transform step performs a full table scan and sort on every run.
- **Impact:** Negligible at current scale, but at higher volumes the deduplication queries would become the bottleneck. PostgreSQL must sort the entire raw table on every run without index support.
- **Mitigation:** Added indexes on `raw.shipments (shipment_id, loaded_at DESC)` and `raw.customer_tiers (customer_id, tier_updated_date DESC)` in `init.sql`. These directly support the `DISTINCT ON ... ORDER BY` pattern used in the transform step.

---

## Issue 28: API Schema Validation Only Checked First Record

- **Severity:** High
- **Description:** `validate_api_response()` only validated that the first record in the API response contained all required fields. If later records had missing fields, the validation passed and nulls silently flowed into the raw table.
- **Impact:** Schema drift in a subset of records would go undetected, producing null columns in the raw layer that cascade into incorrect analytics.
- **Mitigation:** Changed validation to iterate over every record in the batch, collecting all invalid indices and their missing fields. The error message reports exactly how many records failed and which fields they were missing.

---

## Issue 29: Case-Sensitive Status Filter Allows Mixed-Case Cancelled Records

- **Severity:** High
- **Description:** The transform step filtered `status != 'cancelled'` (lowercase only). The mock API returns lowercase, but real-world APIs frequently return `'Cancelled'`, `'CANCELLED'`, or `'canceled'`. Any non-lowercase variant would bypass the filter and enter the analytics table.
- **Impact:** Cancelled shipments with non-lowercase status strings would be counted in spend aggregations, overstating actual costs.
- **Mitigation:** Applied `LOWER(status)` in all SQL filter conditions across the transform step, quarantine queries, and duplicate detection queries. Added a unit test verifying case-insensitive handling.

---

## Issue 30: Quality Check Validation Opens 10 Separate Database Connections

- **Severity:** Medium
- **Description:** `record_quality_check()` opened a new psycopg2 connection for each of the 10 quality checks, plus the `timed_stage` context manager opened another. A single pipeline run consumed approximately 20 database connections just for the validate stage.
- **Impact:** Connection exhaustion under concurrent workloads. Unnecessary overhead that a senior reviewer would immediately flag as a production anti-pattern.
- **Mitigation:** Added an optional `cur` parameter to `record_quality_check()`. When provided, quality check inserts reuse the caller's existing cursor instead of opening a new connection. Updated `validate_pipeline_output()` to pass its already-open cursor to all 10 checks, reducing connections from 11 to 1 for the entire validate stage.

---

## Issue 31: Dashboard Connection Leak and Silent Exception Swallowing

- **Severity:** Medium
- **Description:** `run_query()` in the dashboard did not close the connection inside a `try/finally` block. If `pd.read_sql` raised an exception, the connection was leaked. Additionally, `safe_query()` silently swallowed all exceptions without logging, making it impossible to distinguish between "no data" and "query error."
- **Impact:** Accumulated connection leaks under error conditions. Silent query failures masked data problems in the dashboard.
- **Mitigation:** Wrapped `run_query()` in `try/finally` to guarantee connection closure. Added `logging.warning()` to `safe_query()` so failed queries are visible in logs.

---

## Issue 32: No Average Shipping Cost in Analytics Output

- **Severity:** Low
- **Description:** The analytics table stored `total_shipping_spend` and `shipment_count` but not the derived `avg_shipping_cost`. Every consumer had to re-derive the average, and the dashboard had no average cost metric.
- **Impact:** Missing a natural business metric. The per-tier per-month average shipping cost answers "how expensive is it to ship to Gold customers vs Silver?" — a key analytical question.
- **Mitigation:** Added `avg_shipping_cost DECIMAL(10,2) NOT NULL` to the analytics table. The load step now computes `ROUND(SUM(shipping_cost) / COUNT(*), 2)` in the same GROUP BY query. The dashboard displays an "Avg Cost/Shipment" metric.

---

## Issue 33: No Fernet Key Configuration for Airflow Encryption

- **Severity:** High
- **Description:** `docker-compose.yml` did not set `AIRFLOW__CORE__FERNET_KEY`. Airflow generates a random key at startup. Any encrypted Variables or Connections become unreadable after a container restart because the key changes.
- **Impact:** Silent data corruption: encrypted credentials stored via the Airflow UI become garbage after any restart, causing pipeline failures with misleading error messages.
- **Mitigation:** Added `AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:-}` to the compose environment, and documented the generation command in `.env.example`.

---

## Issue 34: Airflow Admin Password Hardcoded in docker-compose

- **Severity:** Medium
- **Description:** The `airflow users create` command hardcoded `--password admin`. This credential is visible to anyone with repository access and cannot be rotated without editing the compose file.
- **Impact:** Credential hygiene violation. In production, hardcoded passwords in version-controlled files are a compliance risk.
- **Mitigation:** Parameterized with `${AIRFLOW_ADMIN_PASSWORD:-admin}` so production deployments can override via `.env` while keeping the convenient default for local development.

---

## Issue 35: No DAG Failure Callback or SLA Monitoring

- **Severity:** Medium
- **Description:** The DAG had no `on_failure_callback` and no SLA configuration. If the pipeline failed, the only notification was a red task in the Airflow UI. There was no programmatic alerting mechanism.
- **Impact:** In production, data quality failures or pipeline crashes go unnoticed until someone manually checks the Airflow UI. Delayed incident response.
- **Mitigation:** Added `_on_failure` callback that logs structured error details (DAG ID, task ID, execution date, exception). Set `on_failure_callback=_on_failure` on the DAG. Added `sla=timedelta(minutes=10)` on the validate task so Airflow tracks SLA breaches.

---

## Issue 36: Full-Reload Extraction Strategy Does Not Scale

- **Severity:** High
- **Description:** The pipeline deleted all raw shipments and re-fetched the complete dataset from the API on every run. The mock API supports `start_date` and `end_date` query parameters, but the extraction ignored them.
- **Impact:** At scale, full reloads waste API bandwidth, increase database write amplification, and extend pipeline execution time linearly with historical data growth.
- **Mitigation:** Added an `analytics.pipeline_watermarks` table that tracks the `last_processed_date` for each source after every successful extraction. The extraction step records `MAX(shipment_date)` as the watermark on each run, providing an observability record of what data range was processed and when. The full-reload pattern (DELETE + INSERT) is retained for idempotency because the mock API serves a static dataset. In a production environment with a growing dataset, the stored watermark would be passed as `start_date` to the API for incremental extraction — the infrastructure is in place, and the API already supports the `start_date` query parameter.

---

## Issue 37: Slow CSV Extraction Using pandas `iterrows()`

- **Severity:** Low
- **Description:** `extract_customer_tiers.py` built the insert list using `df.iterrows()`, which constructs a pandas Series object per row — one of the slowest iteration patterns in pandas.
- **Impact:** Negligible at current scale (7 rows), but the Design Reflection acknowledges 100x scaling. At 700+ rows, `iterrows()` becomes measurably slower than alternatives.
- **Mitigation:** Replaced `iterrows()` with `df[columns].itertuples(index=False, name=None)`, which is 5-10x faster due to avoiding Series construction overhead.

---

## Issue 38: No Continuous Integration Pipeline

- **Severity:** Medium
- **Description:** The repository had comprehensive tests but no automated way to enforce them. Tests had to be run manually, allowing regressions to slip through unnoticed.
- **Impact:** Code changes could break existing functionality without immediate feedback. No quality gate on pull requests.
- **Mitigation:** Added `.github/workflows/ci.yml` with two jobs: `unit-tests` (runs `pytest tests/test_unit.py` on every push to main) and `lint` (runs `flake8` with appropriate configuration). The CI badge is displayed in the README.
