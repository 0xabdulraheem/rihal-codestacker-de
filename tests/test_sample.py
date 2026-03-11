import os
import re
import sys
import pytest
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PIPELINE_DB_HOST", "postgres"),
        database=os.environ.get("PIPELINE_DB_NAME", "airflow"),
        user=os.environ.get("PIPELINE_DB_USER", "airflow"),
        password=os.environ.get("PIPELINE_DB_PASSWORD", "airflow"),
        port=int(os.environ.get("PIPELINE_DB_PORT", "5432")),
    )


def _query(sql):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


class TestExtractShipments:
    @pytest.fixture(autouse=True)
    def setup_extraction(self):
        from extract_shipments import extract_shipments_from_api
        extract_shipments_from_api()

    def test_raw_shipments_populated(self):
        rows = _query("SELECT COUNT(*) FROM raw.shipments")
        assert rows[0][0] > 0

    def test_raw_shipments_idempotent(self):
        count_first = _query("SELECT COUNT(*) FROM raw.shipments")[0][0]
        from extract_shipments import extract_shipments_from_api
        extract_shipments_from_api()
        count_second = _query("SELECT COUNT(*) FROM raw.shipments")[0][0]
        assert count_first == count_second

    def test_raw_shipments_preserves_all_fields(self):
        rows = _query(
            "SELECT shipment_id, customer_id, shipping_cost, shipment_date, status "
            "FROM raw.shipments LIMIT 1"
        )
        assert len(rows) == 1
        assert len(rows[0]) == 5


class TestExtractCustomerTiers:
    def test_raw_tiers_populated(self):
        from extract_customer_tiers import extract_customer_tiers_from_csv
        extract_customer_tiers_from_csv()
        rows = _query("SELECT COUNT(*) FROM raw.customer_tiers")
        assert rows[0][0] > 0

    def test_raw_tiers_idempotent(self):
        from extract_customer_tiers import extract_customer_tiers_from_csv
        extract_customer_tiers_from_csv()
        count_first = _query("SELECT COUNT(*) FROM raw.customer_tiers")[0][0]
        extract_customer_tiers_from_csv()
        count_second = _query("SELECT COUNT(*) FROM raw.customer_tiers")[0][0]
        assert count_first == count_second


class TestTransform:
    @pytest.fixture(autouse=True)
    def setup_raw_data(self):
        from extract_shipments import extract_shipments_from_api
        from extract_customer_tiers import extract_customer_tiers_from_csv
        extract_shipments_from_api()
        extract_customer_tiers_from_csv()

    def test_deduplication_removes_duplicate_shipment_ids(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        dupes = _query(
            "SELECT shipment_id, COUNT(*) FROM staging.shipments_deduped "
            "GROUP BY shipment_id HAVING COUNT(*) > 1"
        )
        assert len(dupes) == 0

    def test_negative_costs_filtered(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        negatives = _query(
            "SELECT COUNT(*) FROM staging.shipments_deduped WHERE shipping_cost <= 0"
        )
        assert negatives[0][0] == 0

    def test_cancelled_shipments_excluded(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        cancelled = _query(
            "SELECT COUNT(*) FROM staging.shipments_deduped WHERE status = 'cancelled'"
        )
        assert cancelled[0][0] == 0

    def test_null_customer_id_excluded(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        nulls = _query(
            "SELECT COUNT(*) FROM staging.shipments_deduped WHERE customer_id IS NULL"
        )
        assert nulls[0][0] == 0

    def test_tier_resolution_picks_latest(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        cust002 = _query(
            "SELECT tier FROM staging.customer_tiers_resolved WHERE customer_id = 'CUST002'"
        )
        assert len(cust002) == 1
        assert cust002[0][0] == "Gold"

    def test_unknown_customer_gets_unknown_tier(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        unknown = _query(
            "SELECT tier FROM staging.shipments_enriched WHERE customer_id = 'CUST999'"
        )
        assert len(unknown) == 1
        assert unknown[0][0] == "Unknown"

    def test_enriched_no_null_tiers(self):
        from transform_data import transform_shipment_data
        transform_shipment_data()
        nulls = _query(
            "SELECT COUNT(*) FROM staging.shipments_enriched WHERE tier IS NULL"
        )
        assert nulls[0][0] == 0


class TestLoadAnalytics:
    @pytest.fixture(autouse=True)
    def run_full_pipeline(self):
        from extract_shipments import extract_shipments_from_api
        from extract_customer_tiers import extract_customer_tiers_from_csv
        from transform_data import transform_shipment_data
        extract_shipments_from_api()
        extract_customer_tiers_from_csv()
        transform_shipment_data()

    def test_analytics_populated(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        rows = _query("SELECT COUNT(*) FROM analytics.shipping_spend_by_tier")
        assert rows[0][0] > 0

    def test_analytics_idempotent(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        first_run = _query(
            "SELECT tier, year_month, total_shipping_spend, shipment_count "
            "FROM analytics.shipping_spend_by_tier ORDER BY tier, year_month"
        )
        load_analytics_data()
        second_run = _query(
            "SELECT tier, year_month, total_shipping_spend, shipment_count "
            "FROM analytics.shipping_spend_by_tier ORDER BY tier, year_month"
        )
        assert first_run == second_run

    def test_no_negative_spend_in_analytics(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        negatives = _query(
            "SELECT COUNT(*) FROM analytics.shipping_spend_by_tier "
            "WHERE total_shipping_spend < 0"
        )
        assert negatives[0][0] == 0

    def test_all_spend_positive(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        rows = _query(
            "SELECT total_shipping_spend FROM analytics.shipping_spend_by_tier"
        )
        for row in rows:
            assert float(row[0]) > 0

    def test_shipment_counts_positive(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        rows = _query(
            "SELECT shipment_count FROM analytics.shipping_spend_by_tier"
        )
        for row in rows:
            assert row[0] > 0

    def test_year_month_format(self):
        from load_analytics import load_analytics_data
        load_analytics_data()
        rows = _query(
            "SELECT year_month FROM analytics.shipping_spend_by_tier"
        )
        pattern = re.compile(r"^\d{4}-\d{2}$")
        for row in rows:
            assert pattern.match(row[0])


class TestValidateData:
    @pytest.fixture(autouse=True)
    def run_full_pipeline(self):
        from extract_shipments import extract_shipments_from_api
        from extract_customer_tiers import extract_customer_tiers_from_csv
        from transform_data import transform_shipment_data
        from load_analytics import load_analytics_data
        extract_shipments_from_api()
        extract_customer_tiers_from_csv()
        transform_shipment_data()
        load_analytics_data()

    def test_quality_checks_pass(self):
        from validate_data import validate_pipeline_output
        validate_pipeline_output()

    def test_quality_log_populated(self):
        from validate_data import validate_pipeline_output
        validate_pipeline_output()
        rows = _query(
            "SELECT check_name, check_result FROM analytics.data_quality_log "
            "ORDER BY run_timestamp DESC LIMIT 10"
        )
        assert len(rows) == 10
        for row in rows:
            assert row[1] == "pass"

    def test_pipeline_metrics_populated(self):
        rows = _query(
            "SELECT stage, status FROM analytics.pipeline_metrics "
            "ORDER BY run_timestamp DESC LIMIT 4"
        )
        assert len(rows) >= 4
        for row in rows:
            assert row[1] == "success"


class TestQuarantine:
    @pytest.fixture(autouse=True)
    def run_full_pipeline(self):
        from extract_shipments import extract_shipments_from_api
        from extract_customer_tiers import extract_customer_tiers_from_csv
        from transform_data import transform_shipment_data
        extract_shipments_from_api()
        extract_customer_tiers_from_csv()
        transform_shipment_data()

    def test_quarantine_table_populated(self):
        rows = _query("SELECT COUNT(*) FROM staging.quarantine")
        assert rows[0][0] > 0

    def test_quarantine_has_rejection_reasons(self):
        rows = _query(
            "SELECT DISTINCT rejection_reason FROM staging.quarantine"
        )
        reasons = {row[0] for row in rows}
        assert len(reasons) > 0

    def test_quarantine_captures_negative_cost(self):
        rows = _query(
            "SELECT COUNT(*) FROM staging.quarantine "
            "WHERE rejection_reason = 'invalid_shipping_cost'"
        )
        assert rows[0][0] > 0

    def test_quarantine_captures_null_customer(self):
        rows = _query(
            "SELECT COUNT(*) FROM staging.quarantine "
            "WHERE rejection_reason = 'null_customer_id'"
        )
        assert rows[0][0] > 0

    def test_quarantine_captures_cancelled(self):
        rows = _query(
            "SELECT COUNT(*) FROM staging.quarantine "
            "WHERE rejection_reason = 'cancelled_shipment'"
        )
        assert rows[0][0] > 0

    def test_quarantine_idempotent(self):
        count_first = _query("SELECT COUNT(*) FROM staging.quarantine")[0][0]
        from transform_data import transform_shipment_data
        transform_shipment_data()
        count_second = _query("SELECT COUNT(*) FROM staging.quarantine")[0][0]
        assert count_first == count_second
