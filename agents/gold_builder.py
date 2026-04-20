"""
Gold Builder: Creates business-ready aggregate tables from the Silver layer.
These tables answer real business questions and power dashboards / ML models.
"""

import duckdb
from config import DB_PATH


# ---------- MONTHLY CLAIM SUMMARY ----------
GOLD_MONTHLY_SUMMARY_SQL = """
CREATE OR REPLACE TABLE gold_monthly_claim_summary AS
SELECT
    DATE_TRUNC('month', txn_date)          AS month,
    INSURANCE_TYPE                          AS insurance_type,
    COUNT(*)                                AS total_claims,
    SUM(CLAIM_AMOUNT)                       AS total_claim_amount,
    ROUND(AVG(CLAIM_AMOUNT), 2)             AS avg_claim_amount,
    MAX(CLAIM_AMOUNT)                       AS max_claim_amount,
    COUNT(CASE WHEN CLAIM_STATUS = 'A' THEN 1 END)   AS approved_claims,
    COUNT(CASE WHEN CLAIM_STATUS = 'D' THEN 1 END)   AS denied_claims
FROM silver_claims_enriched
GROUP BY DATE_TRUNC('month', txn_date), INSURANCE_TYPE
ORDER BY month DESC, insurance_type
;
"""


# ---------- STATE RISK SUMMARY ----------
GOLD_STATE_RISK_SQL = """
CREATE OR REPLACE TABLE gold_state_risk_summary AS
SELECT
    STATE                                   AS state,
    COUNT(*)                                AS total_claims,
    SUM(CLAIM_AMOUNT)                       AS total_claim_amount,
    ROUND(AVG(CLAIM_AMOUNT), 2)             AS avg_claim_amount,
    COUNT(CASE WHEN RISK_SEGMENTATION = 'H' THEN 1 END)  AS high_risk_claims,
    COUNT(CASE WHEN ANY_INJURY = 1 THEN 1 END)           AS claims_with_injury,
    ROUND(
        100.0 * COUNT(CASE WHEN RISK_SEGMENTATION = 'H' THEN 1 END) / COUNT(*),
        2
    )                                       AS high_risk_pct
FROM silver_claims_enriched
GROUP BY STATE
ORDER BY total_claim_amount DESC
;
"""


# ---------- AGENT PERFORMANCE ----------
GOLD_AGENT_PERFORMANCE_SQL = """
CREATE OR REPLACE TABLE gold_agent_performance AS
SELECT
    AGENT_ID                                AS agent_id,
    agent_name,
    agent_state,
    COUNT(*)                                AS total_claims_processed,
    SUM(CLAIM_AMOUNT)                       AS total_claim_value,
    ROUND(AVG(CLAIM_AMOUNT), 2)             AS avg_claim_value,
    ROUND(AVG(days_to_report), 2)           AS avg_days_to_report,
    COUNT(CASE WHEN CLAIM_STATUS = 'A' THEN 1 END) AS approved_claims
FROM silver_claims_enriched
WHERE AGENT_ID IS NOT NULL
GROUP BY AGENT_ID, agent_name, agent_state
ORDER BY total_claims_processed DESC
;
"""


# ---------- VENDOR STATISTICS ----------
GOLD_VENDOR_STATS_SQL = """
CREATE OR REPLACE TABLE gold_vendor_statistics AS
SELECT
    VENDOR_ID                               AS vendor_id,
    vendor_name,
    vendor_state,
    COUNT(*)                                AS total_claims,
    SUM(CLAIM_AMOUNT)                       AS total_claim_amount,
    ROUND(AVG(CLAIM_AMOUNT), 2)             AS avg_claim_amount,
    COUNT(CASE WHEN INCIDENT_SEVERITY = 'Total Loss' THEN 1 END) AS total_loss_claims
FROM silver_claims_enriched
WHERE VENDOR_ID IS NOT NULL
GROUP BY VENDOR_ID, vendor_name, vendor_state
ORDER BY total_claim_amount DESC
;
"""


# ---------- FRAUD INDICATORS ----------
GOLD_FRAUD_INDICATORS_SQL = """
CREATE OR REPLACE TABLE gold_fraud_indicators AS
SELECT
    RISK_SEGMENTATION                       AS risk_segment,
    INCIDENT_SEVERITY                       AS incident_severity,
    COUNT(*)                                AS claim_count,
    ROUND(AVG(CLAIM_AMOUNT), 2)             AS avg_claim_amount,
    COUNT(CASE WHEN POLICE_REPORT_AVAILABLE = 0 THEN 1 END)  AS no_police_report,
    COUNT(CASE WHEN days_to_report > 7 THEN 1 END)           AS late_reports,
    COUNT(CASE WHEN authority_contacted = 'None' THEN 1 END) AS no_authority_contact
FROM silver_claims_enriched
GROUP BY RISK_SEGMENTATION, INCIDENT_SEVERITY
ORDER BY claim_count DESC
;
"""


def build_gold_layer():
    """Runs all Gold aggregations in order."""
    conn = duckdb.connect(DB_PATH)
    print(f"\n🗄️  Connected to DuckDB: {DB_PATH}")
    print("\n🏆 Building Gold layer...\n")

    gold_tables = [
        ("gold_monthly_claim_summary",  GOLD_MONTHLY_SUMMARY_SQL),
        ("gold_state_risk_summary",     GOLD_STATE_RISK_SQL),
        ("gold_agent_performance",      GOLD_AGENT_PERFORMANCE_SQL),
        ("gold_vendor_statistics",      GOLD_VENDOR_STATS_SQL),
        ("gold_fraud_indicators",       GOLD_FRAUD_INDICATORS_SQL),
    ]

    for table_name, sql in gold_tables:
        conn.execute(sql)
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        print(f"   ✅ {table_name}: {row_count:,} rows")

    # Summary of all layers
    print("\n📋 All tables in warehouse:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for t in tables:
        print(f"   • {t[0]}")

    conn.close()
    print("\n✅ Gold layer build complete!\n")


if __name__ == "__main__":
    build_gold_layer()