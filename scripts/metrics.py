import logging
import time
import uuid
from contextlib import contextmanager
from db import get_connection, get_cursor

logger = logging.getLogger("airflow.task")


def record_metric(stage, rows_processed, rows_rejected=0, duration_seconds=None, status="success"):
    run_id = f"{stage}_{uuid.uuid4().hex[:12]}"
    with get_connection() as conn:
        with get_cursor(conn) as cur:
            cur.execute(
                """
                INSERT INTO analytics.pipeline_metrics
                    (run_id, stage, rows_processed, rows_rejected, duration_seconds, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (run_id, stage, rows_processed, rows_rejected, duration_seconds, status),
            )
            conn.commit()
    logger.info(
        "Metric recorded: stage=%s rows=%d rejected=%d duration=%.3fs status=%s",
        stage, rows_processed, rows_rejected, duration_seconds or 0, status,
    )


def record_quality_check(check_name, passed, details=None):
    check_result = "pass" if passed else "fail"
    with get_connection() as conn:
        with get_cursor(conn) as cur:
            cur.execute(
                """
                INSERT INTO analytics.data_quality_log
                    (check_name, check_result, details)
                VALUES (%s, %s, %s)
                """,
                (check_name, check_result, details),
            )
            conn.commit()
    level = logging.INFO if passed else logging.WARNING
    logger.log(level, "Quality check [%s]: %s — %s", check_name, check_result, details or "")


@contextmanager
def timed_stage(stage_name):
    start = time.time()
    result = {"rows_processed": 0, "rows_rejected": 0, "status": "success"}
    try:
        yield result
    except Exception:
        result["status"] = "failure"
        raise
    finally:
        duration = time.time() - start
        try:
            record_metric(
                stage_name,
                result["rows_processed"],
                result["rows_rejected"],
                duration,
                result["status"],
            )
        except Exception as metric_exc:
            logger.error("Failed to record metric for %s: %s", stage_name, metric_exc)
