import logging
from db import get_connection, get_cursor

logger = logging.getLogger("airflow.task")


def transform_shipment_data():
    logger.info("Starting data transformation")

    with get_connection() as conn:
        with get_cursor(conn) as cur:
            cur.execute("BEGIN;")

            cur.execute("DELETE FROM staging.shipments_deduped;")
            cur.execute(
                """
                INSERT INTO staging.shipments_deduped
                    (shipment_id, customer_id, shipping_cost, shipment_date, status)
                SELECT DISTINCT ON (shipment_id)
                    shipment_id,
                    customer_id,
                    shipping_cost,
                    shipment_date,
                    status
                FROM raw.shipments
                WHERE shipment_id IS NOT NULL
                  AND shipping_cost > 0
                  AND status != 'cancelled'
                  AND customer_id IS NOT NULL
                ORDER BY shipment_id, loaded_at DESC
                """
            )
            cur.execute("SELECT COUNT(*) FROM staging.shipments_deduped")
            deduped_count = cur.fetchone()[0]
            logger.info("Deduplicated shipments: %d valid records", deduped_count)

            cur.execute("DELETE FROM staging.customer_tiers_resolved;")
            cur.execute(
                """
                INSERT INTO staging.customer_tiers_resolved
                    (customer_id, customer_name, tier, tier_updated_date)
                SELECT DISTINCT ON (customer_id)
                    customer_id,
                    customer_name,
                    tier,
                    tier_updated_date
                FROM raw.customer_tiers
                WHERE customer_id IS NOT NULL
                ORDER BY customer_id, tier_updated_date DESC
                """
            )
            cur.execute("SELECT COUNT(*) FROM staging.customer_tiers_resolved")
            tier_count = cur.fetchone()[0]
            logger.info("Resolved customer tiers: %d unique customers", tier_count)

            cur.execute("DELETE FROM staging.shipments_enriched;")
            cur.execute(
                """
                INSERT INTO staging.shipments_enriched
                    (shipment_id, customer_id, shipping_cost, shipment_date,
                     status, tier, customer_name)
                SELECT
                    s.shipment_id,
                    s.customer_id,
                    s.shipping_cost,
                    s.shipment_date,
                    s.status,
                    COALESCE(t.tier, 'Unknown') AS tier,
                    t.customer_name
                FROM staging.shipments_deduped s
                LEFT JOIN staging.customer_tiers_resolved t
                    ON s.customer_id = t.customer_id
                """
            )
            cur.execute("SELECT COUNT(*) FROM staging.shipments_enriched")
            enriched_count = cur.fetchone()[0]
            logger.info("Enriched shipments: %d records ready for analytics", enriched_count)

            conn.commit()

    logger.info("Data transformation completed")
