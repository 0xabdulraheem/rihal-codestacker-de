import os
import logging
import pandas as pd
from db import get_connection, get_cursor

logger = logging.getLogger("airflow.task")

CSV_PATH = os.environ.get("CUSTOMER_TIERS_CSV", "/opt/airflow/data/customer_tiers.csv")


def extract_customer_tiers_from_csv():
    logger.info("Starting customer tier extraction from %s", CSV_PATH)

    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"Customer tiers CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    required_columns = {"customer_id", "customer_name", "tier", "tier_updated_date"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    df = df.dropna(subset=["customer_id", "tier"])
    logger.info("Loaded %d valid tier records from CSV", len(df))

    with get_connection() as conn:
        with get_cursor(conn) as cur:
            cur.execute("BEGIN;")
            cur.execute("DELETE FROM raw.customer_tiers;")
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO raw.customer_tiers
                        (customer_id, customer_name, tier, tier_updated_date)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        row["customer_id"],
                        row["customer_name"],
                        row["tier"],
                        row["tier_updated_date"],
                    ),
                )
            conn.commit()

    logger.info("Customer tier extraction completed: %d rows loaded into raw layer", len(df))
