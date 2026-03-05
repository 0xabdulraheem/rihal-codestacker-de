import logging
from db import get_connection, get_cursor
from metrics import timed_stage

logger = logging.getLogger("airflow.task")


def _quarantine_rejected_shipments(cur):
    cur.execute(
        """
        INSERT INTO staging.quarantine (source_table, record_data, rejection_reason)
        SELECT
            'raw.shipments',
            jsonb_build_object(
                'shipment_id', shipment_id,
                'customer_id', customer_id,
                'shipping_cost', shipping_cost,
                'shipment_date', shipment_date,
                'status', status
            ),
            CASE
                WHEN shipment_id IS NULL THEN 'null_shipment_id'
                WHEN customer_id IS NULL THEN 'null_customer_id'
                WHEN shipping_cost IS NULL OR shipping_cost <= 0 THEN 'invalid_shipping_cost'
                WHEN LOWER(status) = 'cancelled' THEN 'cancelled_shipment'
                ELSE 'unknown'
            END
        FROM raw.shipments
        WHERE shipment_id IS NULL
           OR customer_id IS NULL
           OR shipping_cost IS NULL
           OR shipping_cost <= 0
           OR LOWER(status) = 'cancelled'
        """
    )
    quarantined = cur.rowcount
    if quarantined > 0:
        logger.warning("Quarantined %d rejected shipment records", quarantined)
    return quarantined


def _quarantine_duplicate_shipments(cur):
    cur.execute(
        """
        INSERT INTO staging.quarantine (source_table, record_data, rejection_reason)
        SELECT
            'raw.shipments',
            jsonb_build_object(
                'shipment_id', r.shipment_id,
                'customer_id', r.customer_id,
                'shipping_cost', r.shipping_cost,
                'shipment_date', r.shipment_date,
                'status', r.status,
                'loaded_at', r.loaded_at
            ),
            'duplicate_shipment_id'
        FROM raw.shipments r
        INNER JOIN (
            SELECT shipment_id, MAX(loaded_at) AS max_loaded_at
            FROM raw.shipments
            WHERE shipment_id IS NOT NULL
              AND customer_id IS NOT NULL
              AND shipping_cost > 0
              AND LOWER(status) != 'cancelled'
            GROUP BY shipment_id
            HAVING COUNT(*) > 1
        ) dups ON r.shipment_id = dups.shipment_id
                AND r.loaded_at < dups.max_loaded_at
        WHERE r.customer_id IS NOT NULL
          AND r.shipping_cost > 0
          AND LOWER(r.status) != 'cancelled'
        """
    )
    quarantined = cur.rowcount
    if quarantined > 0:
        logger.warning("Quarantined %d duplicate shipment records (kept latest)", quarantined)
    return quarantined


def transform_shipment_data():
    logger.info("Starting data transformation")

    with timed_stage("transform_data") as stage_metrics:
        with get_connection() as conn:
            with get_cursor(conn) as cur:
                cur.execute("SELECT COUNT(*) FROM raw.shipments")
                raw_count = cur.fetchone()[0]

                cur.execute(
                    "DELETE FROM staging.quarantine WHERE source_table LIKE 'raw.%%';"
                )

                rejected = _quarantine_rejected_shipments(cur)
                dup_rejected = _quarantine_duplicate_shipments(cur)

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
                      AND LOWER(status) != 'cancelled'
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

        total_rejected = rejected + dup_rejected
        stage_metrics["rows_processed"] = enriched_count
        stage_metrics["rows_rejected"] = total_rejected

    logger.info(
        "Data transformation completed: %d accepted, %d quarantined (%d invalid, %d duplicates)",
        enriched_count, total_rejected, rejected, dup_rejected,
    )
