import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

from extract_shipments import extract_shipments_from_api
from extract_customer_tiers import extract_customer_tiers_from_csv
from transform_data import transform_shipment_data
from load_analytics import load_analytics_data

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="shipment_analytics_pipeline",
    default_args=default_args,
    description="ETL pipeline: shipment spend analytics by customer tier per month",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "shipments"],
) as dag:

    extract_shipments = PythonOperator(
        task_id="extract_shipments",
        python_callable=extract_shipments_from_api,
    )

    extract_tiers = PythonOperator(
        task_id="extract_customer_tiers",
        python_callable=extract_customer_tiers_from_csv,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_shipment_data,
    )

    load_analytics = PythonOperator(
        task_id="load_analytics",
        python_callable=load_analytics_data,
    )

    [extract_shipments, extract_tiers] >> transform >> load_analytics
