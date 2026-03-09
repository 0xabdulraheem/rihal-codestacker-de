import os
import json
import logging
from pathlib import Path
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Shipment Analytics Pipeline",
    layout="wide",
)

DEMO_DIR = Path(__file__).parent / "demo_data"
LIVE_MODE = False

try:
    import psycopg2
    DB_PARAMS = {
        "host": os.environ.get("PIPELINE_DB_HOST", "localhost"),
        "database": os.environ.get("PIPELINE_DB_NAME", "airflow"),
        "user": os.environ.get("PIPELINE_DB_USER", "airflow"),
        "password": os.environ.get("PIPELINE_DB_PASSWORD", "airflow"),
        "port": int(os.environ.get("PIPELINE_DB_PORT", "5432")),
        "connect_timeout": 3,
    }
    conn = psycopg2.connect(**DB_PARAMS)
    conn.close()
    LIVE_MODE = True
except Exception:
    LIVE_MODE = False


def run_query(sql):
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def safe_query(sql):
    try:
        return run_query(sql)
    except Exception as exc:
        logger.warning("Dashboard query failed: %s", exc)
        return pd.DataFrame()


def load_demo(filename):
    path = DEMO_DIR / filename
    if path.exists():
        with open(path) as f:
            return pd.DataFrame(json.load(f))
    return pd.DataFrame()


st.title("Shipment Analytics Pipeline")
if LIVE_MODE:
    st.caption("Connected to live PostgreSQL database")
else:
    st.caption("Displaying pipeline output from latest run — run via docker-compose for live database connection")

tab_analytics, tab_quality, tab_metrics, tab_lineage, tab_quarantine = st.tabs(
    ["Analytics Output", "Data Quality", "Pipeline Metrics", "Data Lineage", "Quarantine"]
)

with tab_analytics:
    st.header("Shipping Spend by Customer Tier")

    if LIVE_MODE:
        df = safe_query(
            "SELECT tier, year_month, total_shipping_spend, shipment_count, "
            "avg_shipping_cost, calculated_at "
            "FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier"
        )
    else:
        df = load_demo("analytics.json")

    if df.empty:
        st.warning("No analytics data found. Run the pipeline first.")
    else:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Spend", f"${df['total_shipping_spend'].sum():,.2f}")
        col2.metric("Total Shipments", f"{df['shipment_count'].sum():,}")
        avg_cost = df['total_shipping_spend'].sum() / max(df['shipment_count'].sum(), 1)
        col3.metric("Avg Cost/Shipment", f"${avg_cost:,.2f}")
        col4.metric("Tier-Month Combinations", len(df))

        st.subheader("Spend Breakdown")

        pivot = df.pivot_table(
            index="year_month",
            columns="tier",
            values="total_shipping_spend",
            aggfunc="sum",
            fill_value=0,
        )
        st.bar_chart(pivot)

        st.subheader("Detailed Results")
        st.dataframe(
            df.style.format({
                "total_shipping_spend": "${:,.2f}",
                "shipment_count": "{:,}",
            }),
            use_container_width=True,
            hide_index=True,
        )

with tab_quality:
    st.header("Data Quality Checks")

    if LIVE_MODE:
        dq = safe_query(
            "SELECT check_name, check_result, details, run_timestamp "
            "FROM analytics.data_quality_log ORDER BY run_timestamp DESC LIMIT 50"
        )
    else:
        dq = load_demo("quality_checks.json")
        if not dq.empty:
            dq["run_timestamp"] = pd.to_datetime(dq["run_timestamp"])

    if dq.empty:
        st.info("No quality check results yet.")
    else:
        latest_run = dq["run_timestamp"].max()
        latest_checks = dq[dq["run_timestamp"] == latest_run]

        passed = (latest_checks["check_result"] == "pass").sum()
        failed = (latest_checks["check_result"] == "fail").sum()
        total = len(latest_checks)

        col1, col2, col3 = st.columns(3)
        col1.metric("Checks Passed", f"{passed}/{total}")
        col2.metric("Checks Failed", str(failed))
        col3.metric("Last Run", latest_run.strftime("%Y-%m-%d %H:%M:%S"))

        if failed == 0:
            st.success(f"All {total} quality checks passed")
        else:
            st.error(f"{failed} quality check(s) failed")

        st.subheader("Latest Check Results")
        for _, row in latest_checks.iterrows():
            icon = "+" if row["check_result"] == "pass" else "-"
            st.text(f"  [{icon}] {row['check_name']}: {row['details']}")

        st.subheader("Check History")
        st.dataframe(dq, use_container_width=True, hide_index=True)

with tab_metrics:
    st.header("Pipeline Execution Metrics")

    if LIVE_MODE:
        pm = safe_query(
            "SELECT run_id, stage, rows_processed, rows_rejected, "
            "duration_seconds, status, run_timestamp "
            "FROM analytics.pipeline_metrics ORDER BY run_timestamp DESC LIMIT 100"
        )
    else:
        pm = load_demo("pipeline_metrics.json")
        if not pm.empty:
            pm["run_timestamp"] = pd.to_datetime(pm["run_timestamp"])

    if pm.empty:
        st.info("No pipeline metrics recorded yet.")
    else:
        latest_ts = pm["run_timestamp"].max()
        latest = pm[pm["run_timestamp"] >= latest_ts - pd.Timedelta(seconds=30)]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Rows Processed", f"{latest['rows_processed'].sum():,}")
        col2.metric("Total Rows Rejected", f"{latest['rows_rejected'].sum():,}")
        col3.metric(
            "Pipeline Duration",
            f"{latest['duration_seconds'].sum():.1f}s",
        )
        col4.metric("Stages", len(latest))

        st.subheader("Stage Breakdown (Latest Run)")
        for _, row in latest.sort_values("run_timestamp").iterrows():
            st.text(
                f"  {row['stage']:.<30s} "
                f"processed={row['rows_processed']:<6} "
                f"rejected={row['rows_rejected']:<6} "
                f"duration={row['duration_seconds']:.3f}s "
                f"[{row['status']}]"
            )

        st.subheader("Full History")
        st.dataframe(pm, use_container_width=True, hide_index=True)

with tab_lineage:
    st.header("Data Lineage")
    st.caption("Row counts through each pipeline layer")

    if LIVE_MODE:
        counts = {}
        for table, label in [
            ("raw.shipments", "Raw Shipments"),
            ("raw.customer_tiers", "Raw Customer Tiers"),
            ("staging.shipments_deduped", "Staging: Deduped"),
            ("staging.customer_tiers_resolved", "Staging: Tiers Resolved"),
            ("staging.shipments_enriched", "Staging: Enriched"),
            ("analytics.shipping_spend_by_tier", "Analytics Output"),
        ]:
            result = safe_query(f"SELECT COUNT(*) as cnt FROM {table}")
            counts[label] = result["cnt"].iloc[0] if not result.empty else 0
    else:
        counts = {
            "Raw Shipments": 21,
            "Raw Customer Tiers": 7,
            "Staging: Deduped": 15,
            "Staging: Tiers Resolved": 6,
            "Staging: Enriched": 15,
            "Analytics Output": 10,
        }

    if all(v == 0 for v in counts.values()):
        st.info("No data in pipeline tables. Run the pipeline first.")
    else:
        lineage_df = pd.DataFrame(
            list(counts.items()), columns=["Layer", "Row Count"]
        )
        st.bar_chart(lineage_df.set_index("Layer"))

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Source Layer")
            st.metric("Raw Shipments", counts["Raw Shipments"])
            st.metric("Raw Customer Tiers", counts["Raw Customer Tiers"])

        with col2:
            st.subheader("Processing Funnel")
            raw = counts["Raw Shipments"]
            deduped = counts["Staging: Deduped"]
            enriched = counts["Staging: Enriched"]
            if raw > 0:
                st.metric("After Deduplication", deduped, f"{deduped - raw} filtered")
                st.metric("After Enrichment", enriched)
                st.metric("Analytics Groups", counts["Analytics Output"])

with tab_quarantine:
    st.header("Quarantined Records")
    st.caption("Records rejected during transformation with specific rejection reasons")

    if LIVE_MODE:
        qr = safe_query(
            "SELECT quarantine_id, source_table, record_data, rejection_reason, quarantined_at "
            "FROM staging.quarantine ORDER BY quarantined_at DESC LIMIT 200"
        )
    else:
        qr = load_demo("quarantine.json")
        if not qr.empty:
            qr["quarantined_at"] = pd.to_datetime(qr["quarantined_at"])

    if qr.empty:
        st.info("No quarantined records. All source data passed validation.")
    else:
        col1, col2 = st.columns(2)
        col1.metric("Total Quarantined", len(qr))

        reason_counts = qr["rejection_reason"].value_counts()
        col2.metric("Rejection Categories", len(reason_counts))

        st.subheader("Rejection Breakdown")
        reason_df = reason_counts.reset_index()
        reason_df.columns = ["Reason", "Count"]
        st.bar_chart(reason_df.set_index("Reason"))

        st.subheader("Quarantined Records Detail")
        st.dataframe(qr, use_container_width=True, hide_index=True)
