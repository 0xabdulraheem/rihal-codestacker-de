import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


@pytest.fixture
def sample_api_response():
    return {
        "data": [
            {"shipment_id": "SHP001", "customer_id": "CUST001", "shipping_cost": 25.50, "shipment_date": "2024-01-15", "status": "delivered"},
            {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 45.00, "shipment_date": "2024-01-16", "status": "delivered"},
            {"shipment_id": "SHP002", "customer_id": "CUST002", "shipping_cost": 47.00, "shipment_date": "2024-01-16", "status": "delivered"},
            {"shipment_id": "SHP003", "customer_id": "CUST003", "shipping_cost": -5.00, "shipment_date": "2024-01-20", "status": "delivered"},
            {"shipment_id": "SHP004", "customer_id": None, "shipping_cost": 30.00, "shipment_date": "2024-01-25", "status": "delivered"},
            {"shipment_id": "SHP005", "customer_id": "CUST005", "shipping_cost": 50.00, "shipment_date": "2024-03-10", "status": "cancelled"},
            {"shipment_id": "SHP006", "customer_id": "CUST004", "shipping_cost": 0.00, "shipment_date": "2024-02-25", "status": "delivered"},
        ],
        "count": 7,
        "timestamp": "2024-01-01T00:00:00",
    }


@pytest.fixture
def sample_tiers_csv(tmp_path):
    csv_content = (
        "customer_id,customer_name,tier,tier_updated_date\n"
        "CUST001,Acme Corp,Gold,2024-01-01\n"
        "CUST002,Global Imports,Platinum,2024-01-01\n"
        "CUST002,Global Imports,Gold,2024-02-15\n"
        "CUST003,Smith Sons,Silver,2024-01-01\n"
    )
    csv_file = tmp_path / "customer_tiers.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)
