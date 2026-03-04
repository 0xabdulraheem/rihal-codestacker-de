import logging
from db import get_connection, get_cursor
from metrics import record_quality_check, timed_stage

logger = logging.getLogger("airflow.task")


def _count(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def _check(failures, name, passed, details):
    record_quality_check(name, passed, details)
    if not passed:
        failures.append(name)


def validate_pipeline_output():
    logger.info("Starting data quality validation")
    failures = []

    with timed_stage("validate_data_quality") as stage_metrics:
        with get_connection() as conn:
            with get_cursor(conn) as cur:

                raw_count = _count(cur, "SELECT COUNT(*) FROM raw.shipments")
                deduped_count = _count(cur, "SELECT COUNT(*) FROM staging.shipments_deduped")
                enriched_count = _count(cur, "SELECT COUNT(*) FROM staging.shipments_enriched")
                analytics_count = _count(cur, "SELECT COUNT(*) FROM analytics.shipping_spend_by_tier")

                _check(failures, "raw_not_empty", raw_count > 0,
                       f"raw.shipments has {raw_count} rows")

                _check(failures, "deduped_less_than_raw", deduped_count <= raw_count,
                       f"deduped={deduped_count} raw={raw_count}")

                _check(failures, "enriched_matches_deduped", enriched_count == deduped_count,
                       f"enriched={enriched_count} deduped={deduped_count}")

                _check(failures, "analytics_not_empty", analytics_count > 0,
                       f"analytics has {analytics_count} tier-month combinations")

                neg_count = _count(
                    cur,
                    "SELECT COUNT(*) FROM staging.shipments_deduped WHERE shipping_cost <= 0",
                )
                _check(failures, "no_negative_costs_in_staging", neg_count == 0,
                       f"found {neg_count} non-positive costs")

                dup_count = _count(
                    cur,
                    "SELECT COUNT(*) FROM ("
                    "  SELECT shipment_id FROM staging.shipments_deduped"
                    "  GROUP BY shipment_id HAVING COUNT(*) > 1"
                    ") dupes",
                )
                _check(failures, "no_duplicate_shipment_ids", dup_count == 0,
                       f"found {dup_count} duplicate shipment IDs")

                null_tier_count = _count(
                    cur,
                    "SELECT COUNT(*) FROM staging.shipments_enriched WHERE tier IS NULL",
                )
                _check(failures, "no_null_tiers_in_enriched", null_tier_count == 0,
                       f"found {null_tier_count} null tiers")

                neg_spend = _count(
                    cur,
                    "SELECT COUNT(*) FROM analytics.shipping_spend_by_tier WHERE total_shipping_spend <= 0",
                )
                _check(failures, "no_non_positive_spend_in_analytics", neg_spend == 0,
                       f"found {neg_spend} non-positive spend rows")

                cur.execute(
                    "SELECT SUM(total_shipping_spend) FROM analytics.shipping_spend_by_tier"
                )
                analytics_total = cur.fetchone()[0] or 0
                cur.execute(
                    "SELECT SUM(shipping_cost) FROM staging.shipments_enriched"
                )
                enriched_total = cur.fetchone()[0] or 0
                totals_match = abs(float(analytics_total) - float(enriched_total)) < 0.01
                _check(failures, "analytics_total_matches_enriched", totals_match,
                       f"analytics_sum={analytics_total} enriched_sum={enriched_total}")

        stage_metrics["rows_processed"] = 9 - len(failures)
        stage_metrics["rows_rejected"] = len(failures)

    if failures:
        raise ValueError(f"Data quality checks failed: {failures}")

    logger.info("All 9 data quality checks passed")
