"""
Silver Cleaner: Transforms raw Bronze data into cleaned, business-ready Silver tables.

Cleaning operations:
  - Drop columns with too many nulls (e.g., ADDRESS_LINE2)
  - Fill nulls with sensible defaults
  - Convert date strings to DATE types
  - Remove duplicates
  - Join tables into an enriched claims view
"""

import duckdb
from config import DB_PATH


# ---------- SQL-BASED CLEANING ----------
# We use SQL directly because DuckDB is blazing fast with it
# and it's how real data warehouses work.

SILVER_INSURANCE_SQL = """
CREATE OR REPLACE TABLE silver_insurance AS
SELECT DISTINCT
    TRANSACTION_ID,
    CUSTOMER_ID,
    POLICY_NUMBER,

    -- Convert date strings to proper DATE types
    CAST(TXN_DATE_TIME   AS DATE) AS txn_date,
    CAST(POLICY_EFF_DT   AS DATE) AS policy_start_date,
    CAST(LOSS_DT         AS DATE) AS loss_date,
    CAST(REPORT_DT       AS DATE) AS report_date,

    -- Derived column: how many days between loss and report?
    DATE_DIFF('day', CAST(LOSS_DT AS DATE), CAST(REPORT_DT AS DATE)) AS days_to_report,

    INSURANCE_TYPE,
    PREMIUM_AMOUNT,
    CLAIM_AMOUNT,

    -- Customer info (dropping ADDRESS_LINE2 - 85% nulls)
    CUSTOMER_NAME,
    ADDRESS_LINE1,
    COALESCE(CITY, 'Unknown')  AS city,
    STATE,
    POSTAL_CODE,
    MARITAL_STATUS,
    AGE,
    TENURE,
    EMPLOYMENT_STATUS,
    NO_OF_FAMILY_MEMBERS,
    RISK_SEGMENTATION,
    HOUSE_TYPE,
    SOCIAL_CLASS,

    -- Fill nulls for optional fields
    COALESCE(CUSTOMER_EDUCATION_LEVEL, 'Unknown') AS customer_education_level,

    -- Claim details
    CLAIM_STATUS,
    INCIDENT_SEVERITY,
    COALESCE(AUTHORITY_CONTACTED, 'None')  AS authority_contacted,
    ANY_INJURY,
    POLICE_REPORT_AVAILABLE,
    INCIDENT_STATE,
    COALESCE(INCIDENT_CITY, 'Unknown')  AS incident_city,
    INCIDENT_HOUR_OF_THE_DAY,

    -- Relationships
    AGENT_ID,
    VENDOR_ID

FROM bronze_insurance
WHERE CITY IS NOT NULL              -- drop the few rows with null CITY
  AND CLAIM_AMOUNT IS NOT NULL      -- drop rows without claim amount
  AND CLAIM_AMOUNT > 0              -- keep only valid claims
;
"""


SILVER_EMPLOYEES_SQL = """
CREATE OR REPLACE TABLE silver_employees AS
SELECT DISTINCT
    AGENT_ID,
    AGENT_NAME,
    CAST(DATE_OF_JOINING AS DATE) AS date_of_joining,
    ADDRESS_LINE1,
    COALESCE(CITY, 'Unknown')  AS city,
    STATE,
    POSTAL_CODE
FROM bronze_employees
WHERE CITY IS NOT NULL
;
"""


SILVER_VENDORS_SQL = """
CREATE OR REPLACE TABLE silver_vendors AS
SELECT DISTINCT
    VENDOR_ID,
    VENDOR_NAME,
    ADDRESS_LINE1,
    COALESCE(CITY, 'Unknown')  AS city,
    STATE,
    POSTAL_CODE
FROM bronze_vendors
WHERE CITY IS NOT NULL
;
"""


# Joined "master" view for the Gold layer to use
SILVER_CLAIMS_ENRICHED_SQL = """
CREATE OR REPLACE TABLE silver_claims_enriched AS
SELECT
    i.*,

    -- Agent details
    e.AGENT_NAME         AS agent_name,
    e.date_of_joining    AS agent_joining_date,
    e.state              AS agent_state,

    -- Vendor details
    v.VENDOR_NAME        AS vendor_name,
    v.state              AS vendor_state

FROM silver_insurance   AS i
LEFT JOIN silver_employees AS e  ON i.AGENT_ID  = e.AGENT_ID
LEFT JOIN silver_vendors   AS v  ON i.VENDOR_ID = v.VENDOR_ID
;
"""


def build_silver_layer():
    """Runs all Silver transformations in order."""
    conn = duckdb.connect(DB_PATH)
    print(f"\n🗄️  Connected to DuckDB: {DB_PATH}")
    print("\n🧹 Building Silver layer...\n")

    # Step 1 — clean individual tables
    steps = [
        ("silver_insurance",         SILVER_INSURANCE_SQL),
        ("silver_employees",         SILVER_EMPLOYEES_SQL),
        ("silver_vendors",           SILVER_VENDORS_SQL),
        ("silver_claims_enriched",   SILVER_CLAIMS_ENRICHED_SQL),
    ]

    for table_name, sql in steps:
        conn.execute(sql)
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        print(f"   ✅ {table_name}: {row_count:,} rows")

    # Summary
    print("\n📋 All tables in warehouse:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for t in tables:
        print(f"   • {t[0]}")

    conn.close()
    print("\n✅ Silver layer build complete!\n")


if __name__ == "__main__":
    build_silver_layer()