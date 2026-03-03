import os
import logging
import requests
from time import sleep
from psycopg2.extras import execute_values
from db import get_connection, get_cursor
from metrics import timed_stage

logger = logging.getLogger("airflow.task")

API_BASE_URL = os.environ.get("SHIPMENT_API_URL", "http://api:8000")
MAX_RETRIES = 3
RETRY_BACKOFF = 2
REQUIRED_FIELDS = {"shipment_id", "customer_id", "shipping_cost", "shipment_date", "status"}


def validate_api_response(payload):
    if not isinstance(payload, dict):
        raise ValueError(f"API response is not a JSON object, got {type(payload).__name__}")
    if "data" not in payload:
        raise ValueError(f"API response missing 'data' key, got keys: {list(payload.keys())}")
    shipments = payload["data"]
    if not isinstance(shipments, list):
        raise ValueError(f"API 'data' field is not a list, got {type(shipments).__name__}")
    if len(shipments) == 0:
        logger.warning("API returned 0 shipments")
        return shipments
    invalid = []
    for i, record in enumerate(shipments):
        missing = REQUIRED_FIELDS - set(record.keys())
        if missing:
            invalid.append((i, missing))
    if invalid:
        raise ValueError(
            f"API shipment records missing required fields in {len(invalid)} record(s): "
            f"{invalid[:5]}"
        )
    return shipments


def fetch_shipments_with_retry():
    url = f"{API_BASE_URL}/api/shipments"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()
            return validate_api_response(payload)
        except (requests.RequestException, KeyError, ValueError) as exc:
            logger.warning("API attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                sleep(RETRY_BACKOFF ** attempt)
            else:
                raise RuntimeError(f"Shipment API unreachable after {MAX_RETRIES} attempts") from exc


def _update_watermark(cur, max_date):
    cur.execute(
        """
        INSERT INTO analytics.pipeline_watermarks (source, last_processed_date, updated_at)
        VALUES ('shipments', %s, CURRENT_TIMESTAMP)
        ON CONFLICT (source)
        DO UPDATE SET last_processed_date = EXCLUDED.last_processed_date,
                      updated_at = CURRENT_TIMESTAMP
        """,
        (max_date,),
    )


def extract_shipments_from_api():
    logger.info("Starting shipment extraction from API")

    with timed_stage("extract_shipments") as metrics:
        shipments = fetch_shipments_with_retry()
        logger.info("Fetched %d shipment records from API", len(shipments))

        with get_connection() as conn:
            with get_cursor(conn) as cur:
                cur.execute("DELETE FROM raw.shipments;")
                rows = [
                    (
                        s.get("shipment_id"),
                        s.get("customer_id"),
                        s.get("shipping_cost"),
                        s.get("shipment_date"),
                        s.get("status"),
                    )
                    for s in shipments
                ]
                execute_values(
                    cur,
                    "INSERT INTO raw.shipments "
                    "(shipment_id, customer_id, shipping_cost, shipment_date, status) "
                    "VALUES %s",
                    rows,
                )

                dates = [s.get("shipment_date") for s in shipments if s.get("shipment_date")]
                if dates:
                    _update_watermark(cur, max(dates))

                conn.commit()

        metrics["rows_processed"] = len(shipments)

    logger.info("Shipment extraction completed: %d rows loaded into raw layer", len(shipments))
