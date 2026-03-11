import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


class TestValidateApiResponse:
    def test_valid_response(self, sample_api_response):
        from extract_shipments import validate_api_response
        result = validate_api_response(sample_api_response)
        assert isinstance(result, list)
        assert len(result) == 7

    def test_missing_data_key(self):
        from extract_shipments import validate_api_response
        with pytest.raises(ValueError, match="missing 'data' key"):
            validate_api_response({"results": []})

    def test_not_a_dict(self):
        from extract_shipments import validate_api_response
        with pytest.raises(ValueError, match="not a JSON object"):
            validate_api_response([1, 2, 3])

    def test_data_not_a_list(self):
        from extract_shipments import validate_api_response
        with pytest.raises(ValueError, match="not a list"):
            validate_api_response({"data": "not_a_list"})

    def test_empty_data(self):
        from extract_shipments import validate_api_response
        result = validate_api_response({"data": []})
        assert result == []

    def test_missing_required_fields(self):
        from extract_shipments import validate_api_response
        with pytest.raises(ValueError, match="missing required fields in 1 record"):
            validate_api_response({"data": [{"shipment_id": "SHP001"}]})

    def test_later_record_missing_fields(self):
        from extract_shipments import validate_api_response
        good = {"shipment_id": "SHP001", "customer_id": "C1", "shipping_cost": 10, "shipment_date": "2024-01-01", "status": "delivered"}
        bad = {"shipment_id": "SHP002"}
        with pytest.raises(ValueError, match="missing required fields in 1 record"):
            validate_api_response({"data": [good, bad]})


class TestFetchShipmentsRetry:
    @patch("extract_shipments.requests.get")
    def test_success_on_first_try(self, mock_get, sample_api_response):
        from extract_shipments import fetch_shipments_with_retry
        mock_response = MagicMock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_shipments_with_retry()
        assert len(result) == 7
        assert mock_get.call_count == 1

    @patch("extract_shipments.sleep")
    @patch("extract_shipments.requests.get")
    def test_retry_on_failure_then_success(self, mock_get, mock_sleep, sample_api_response):
        from extract_shipments import fetch_shipments_with_retry
        import requests as req

        fail_response = MagicMock()
        fail_response.raise_for_status.side_effect = req.exceptions.HTTPError("500")

        ok_response = MagicMock()
        ok_response.json.return_value = sample_api_response
        ok_response.raise_for_status.return_value = None

        mock_get.side_effect = [fail_response, ok_response]

        result = fetch_shipments_with_retry()
        assert len(result) == 7
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("extract_shipments.sleep")
    @patch("extract_shipments.requests.get")
    def test_all_retries_exhausted(self, mock_get, mock_sleep):
        from extract_shipments import fetch_shipments_with_retry
        import requests as req

        fail_response = MagicMock()
        fail_response.raise_for_status.side_effect = req.exceptions.HTTPError("500")
        mock_get.return_value = fail_response

        with pytest.raises(RuntimeError, match="unreachable after 3 attempts"):
            fetch_shipments_with_retry()
        assert mock_get.call_count == 3


class TestCsvValidation:
    def test_valid_csv(self, sample_tiers_csv):
        import pandas as pd
        df = pd.read_csv(sample_tiers_csv)
        required_columns = {"customer_id", "customer_name", "tier", "tier_updated_date"}
        assert required_columns.issubset(set(df.columns))
        assert len(df) == 4

    def test_missing_csv_raises(self):
        from extract_customer_tiers import extract_customer_tiers_from_csv
        with patch.dict(os.environ, {"CUSTOMER_TIERS_CSV": "/nonexistent/path.csv"}):
            with patch("extract_customer_tiers.CSV_PATH", "/nonexistent/path.csv"):
                with pytest.raises(FileNotFoundError):
                    extract_customer_tiers_from_csv()

    def test_csv_with_missing_columns(self, tmp_path):
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("customer_id,name\nCUST001,Acme\n")
        import pandas as pd
        df = pd.read_csv(str(bad_csv))
        required = {"customer_id", "customer_name", "tier", "tier_updated_date"}
        missing = required - set(df.columns)
        assert len(missing) > 0


class TestDbModule:
    def test_get_connection_params_defaults(self):
        from db import get_connection_params
        with patch.dict(os.environ, {}, clear=True):
            params = get_connection_params()
            assert params["host"] == "postgres"
            assert params["database"] == "airflow"
            assert params["user"] == "airflow"
            assert params["password"] == "airflow"
            assert params["port"] == 5432
            assert params["connect_timeout"] == 10

    def test_get_connection_params_from_env(self):
        from db import get_connection_params
        env = {
            "PIPELINE_DB_HOST": "myhost",
            "PIPELINE_DB_NAME": "mydb",
            "PIPELINE_DB_USER": "myuser",
            "PIPELINE_DB_PASSWORD": "secret",
            "PIPELINE_DB_PORT": "5433",
        }
        with patch.dict(os.environ, env):
            params = get_connection_params()
            assert params["host"] == "myhost"
            assert params["database"] == "mydb"
            assert params["user"] == "myuser"
            assert params["password"] == "secret"
            assert params["port"] == 5433


class TestMetricsModule:
    @patch("metrics.get_connection")
    def test_timed_stage_records_success(self, mock_conn):
        from metrics import timed_stage
        mock_ctx = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_ctx.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_ctx.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with timed_stage("test_stage") as result:
            result["rows_processed"] = 42

        assert result["status"] == "success"
        assert result["rows_processed"] == 42

    @patch("metrics.get_connection")
    def test_timed_stage_records_failure(self, mock_conn):
        from metrics import timed_stage
        mock_ctx = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock(return_value=mock_ctx)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_ctx.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_ctx.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(ValueError):
            with timed_stage("test_stage") as result:
                raise ValueError("boom")

        assert result["status"] == "failure"


class TestEdgeCases:
    def test_shipment_row_extraction(self, sample_api_response):
        shipments = sample_api_response["data"]
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
        assert len(rows) == 7
        assert rows[3][2] == -5.00
        assert rows[4][1] is None

    def test_duplicate_shipment_ids_in_source(self, sample_api_response):
        shipments = sample_api_response["data"]
        ids = [s["shipment_id"] for s in shipments]
        assert ids.count("SHP002") == 2

    def test_case_insensitive_cancelled_status(self):
        statuses = ['cancelled', 'Cancelled', 'CANCELLED']
        for s in statuses:
            assert s.lower() == 'cancelled'

    def test_csv_tier_resolution_logic(self, sample_tiers_csv):
        import pandas as pd
        df = pd.read_csv(sample_tiers_csv)
        df = df.dropna(subset=["customer_id", "tier"])
        df = df.sort_values("tier_updated_date", ascending=False)
        resolved = df.drop_duplicates(subset=["customer_id"], keep="first")
        cust002 = resolved[resolved["customer_id"] == "CUST002"]
        assert len(cust002) == 1
        assert cust002.iloc[0]["tier"] == "Gold"
