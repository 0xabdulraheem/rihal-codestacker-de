import logging
from db import get_connection, get_cursor
from metrics import record_quality_check

logger = logging.getLogger("airflow.task")


def _count(cur, sql):
    cur.execute(sql)
    return cur.fetchone()[0]


def validate_pipeline_output():
    logger.info("Starting data quality validation")

    with get_connection() as conn:
        with get_cursor(conn) as cur:

            raw_count = _count(cur, "SELECT COUNT(*) FROM raw.shipments")
            deduped_count = _count(cur, "SELECT COUNT(*) FROM staging.shipments_deduped")
            enriched_count = _count(cur, "SELECT COUNT(*) FROM staging.shipments_enriched")
            analytics_count = _count(cur, "SELECT COUNT(*) FROM analytics.shipping_spend_by_tier")

            record_quality_check(
                "raw_not_empty",
                raw_count > 0,
                f"raw.shipments has {raw_count} rows",
            )

            record_quality_check(
                "deduped_less_than_raw",
                deduped_count <= raw_count,
                f"deduped={deduped_count} raw={raw_count}",
            )

            record_quality_check(
                "enriched_matches_deduped",
                enriched_count == deduped_count,
                f"enriched={enriched_count} deduped={deduped_count}",
            )

            record_quality_check(
                "analytics_not_empty",
                analytics_count > 0,
                f"analytics has {analytics_count} tier-month combinations",
            )

            neg_count = _count(
                cur,
                "SELECT COUNT(*) FROM staging.shipments_deduped WHERE shipping_cost <= 0",
            )
            record_quality_check(
                "no_negative_costs_in_staging",
                neg_count == 0,
                f"found {neg_count} non-positive costs",
            )

            dup_count = _count(
                cur,
                "SELECT COUNT(*) FROM ("
                "  SELECT shipment_id FROM staging.shipments_deduped"
                "  GROUP BY shipment_id HAVING COUNT(*) > 1"
                ") dupes",
            )
            record_quality_check(
                "no_duplicate_shipment_ids",
                dup_count == 0,
                f"found {dup_count} duplicate shipment IDs",
            )

            null_tier_count = _count(
                cur,
                "SELECT COUNT(*) FROM staging.shipments_enriched WHERE tier IS NULL",
            )
            record_quality_check(
                "no_null_tiers_in_enriched",
                null_tier_count == 0,
                f"found {null_tier_count} null tiers",
            )

            neg_spend = _count(
                cur,
                "SELECT COUNT(*) FROM analytics.shipping_spend_by_tier WHERE total_shipping_spend <= 0",
            )
            record_quality_check(
                "no_non_positive_spend_in_analytics",
                neg_spend == 0,
                f"found {neg_spend} non-positive spend rows",
            )

            cur.execute(
                "SELECT SUM(total_shipping_spend) FROM analytics.shipping_spend_by_tier"
            )
            analytics_total = cur.fetchone()[0] or 0
            cur.execute(
                "SELECT SUM(shipping_cost) FROM staging.shipments_enriched"
            )
            enriched_total = cur.fetchone()[0] or 0
            totals_match = abs(float(analytics_total) - float(enriched_total)) < 0.01
            record_quality_check(
                "analytics_total_matches_enriched",
                totals_match,
                f"analytics_sum={analytics_total} enriched_sum={enriched_total}",
            )

    logger.info("Data quality validation completed")
