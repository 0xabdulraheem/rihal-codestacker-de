import os
import logging
import pandas as pd
from psycopg2.extras import execute_values
from db import get_connection, get_cursor
from metrics import timed_stage

logger = logging.getLogger("airflow.task")

CSV_PATH = os.environ.get("CUSTOMER_TIERS_CSV", "/opt/airflow/data/customer_tiers.csv")


def extract_customer_tiers_from_csv():
    logger.info("Starting customer tier extraction from %s", CSV_PATH)

    with timed_stage("extract_customer_tiers") as metrics:
        if not os.path.isfile(CSV_PATH):
            raise FileNotFoundError(f"Customer tiers CSV not found: {CSV_PATH}")

        df = pd.read_csv(CSV_PATH)

        required_columns = {"customer_id", "customer_name", "tier", "tier_updated_date"}
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        original_count = len(df)
        df = df.dropna(subset=["customer_id", "tier"])
        if len(df) == 0:
            raise ValueError("No valid customer tier records after filtering")
        logger.info("Loaded %d valid tier records from CSV", len(df))

        with get_connection() as conn:
            with get_cursor(conn) as cur:
                cur.execute("DELETE FROM raw.customer_tiers;")
                rows = list(
                    df[["customer_id", "customer_name", "tier", "tier_updated_date"]]
                    .itertuples(index=False, name=None)
                )
                execute_values(
                    cur,
                    "INSERT INTO raw.customer_tiers "
                    "(customer_id, customer_name, tier, tier_updated_date) "
                    "VALUES %s",
                    rows,
                )
                conn.commit()

        metrics["rows_processed"] = len(df)
        metrics["rows_rejected"] = original_count - len(df)

    logger.info("Customer tier extraction completed: %d rows loaded into raw layer", len(df))
