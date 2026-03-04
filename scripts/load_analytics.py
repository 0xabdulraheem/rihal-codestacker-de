import logging
from db import get_connection, get_cursor
from metrics import timed_stage

logger = logging.getLogger("airflow.task")


def load_analytics_data():
    logger.info("Starting analytics load")

    with timed_stage("load_analytics") as stage_metrics:
        with get_connection() as conn:
            with get_cursor(conn) as cur:
                cur.execute("BEGIN;")

                cur.execute("DELETE FROM analytics.shipping_spend_by_tier;")

                cur.execute(
                    """
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
                    """
                )
                rows_inserted = cur.rowcount
                logger.info("Inserted %d aggregated rows into analytics", rows_inserted)

                cur.execute(
                    """
                    SELECT tier, year_month, total_shipping_spend, shipment_count
                    FROM analytics.shipping_spend_by_tier
                    ORDER BY year_month, tier
                    """
                )
                for row in cur.fetchall():
                    logger.info(
                        "  %s | %s | spend=%.2f | count=%d",
                        row[0], row[1], float(row[2]), row[3]
                    )

                conn.commit()

        stage_metrics["rows_processed"] = rows_inserted

    logger.info("Analytics load completed")
