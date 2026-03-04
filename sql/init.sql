CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;

CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id   VARCHAR(50),
    customer_id   VARCHAR(50),
    shipping_cost DECIMAL(10,2),
    shipment_date DATE,
    status        VARCHAR(50),
    loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.customer_tiers (
    customer_id      VARCHAR(50),
    customer_name    VARCHAR(200),
    tier             VARCHAR(50),
    tier_updated_date DATE,
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.shipments_deduped (
    shipment_id   VARCHAR(50) PRIMARY KEY,
    customer_id   VARCHAR(50) NOT NULL,
    shipping_cost DECIMAL(10,2) NOT NULL CHECK (shipping_cost > 0),
    shipment_date DATE NOT NULL,
    status        VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.customer_tiers_resolved (
    customer_id      VARCHAR(50) PRIMARY KEY,
    customer_name    VARCHAR(200),
    tier             VARCHAR(50) NOT NULL,
    tier_updated_date DATE
);

CREATE TABLE IF NOT EXISTS staging.shipments_enriched (
    shipment_id   VARCHAR(50) PRIMARY KEY,
    customer_id   VARCHAR(50) NOT NULL,
    shipping_cost DECIMAL(10,2) NOT NULL,
    shipment_date DATE NOT NULL,
    status        VARCHAR(50) NOT NULL,
    tier          VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
    tier                 VARCHAR(50)   NOT NULL,
    year_month           VARCHAR(7)    NOT NULL,
    total_shipping_spend DECIMAL(12,2) NOT NULL,
    shipment_count       INTEGER       NOT NULL,
    calculated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tier, year_month)
);

CREATE TABLE IF NOT EXISTS analytics.pipeline_metrics (
    run_id            VARCHAR(100)  PRIMARY KEY,
    run_timestamp     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stage             VARCHAR(50)   NOT NULL,
    rows_processed    INTEGER       NOT NULL DEFAULT 0,
    rows_rejected     INTEGER       NOT NULL DEFAULT 0,
    duration_seconds  DECIMAL(10,3),
    status            VARCHAR(20)   NOT NULL DEFAULT 'success'
);

CREATE TABLE IF NOT EXISTS analytics.data_quality_log (
    check_id          SERIAL        PRIMARY KEY,
    run_timestamp     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    check_name        VARCHAR(100)  NOT NULL,
    check_result      VARCHAR(20)   NOT NULL,
    details           TEXT
);
