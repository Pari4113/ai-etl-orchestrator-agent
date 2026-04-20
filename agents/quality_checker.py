"""
Data Quality Checker: Runs automated checks on each layer of the pipeline.

Check Types:
  - Completeness: columns within acceptable null %
  - Uniqueness: primary key columns have no duplicates
  - Validity: numeric ranges, date validity
  - Referential: foreign keys exist in parent tables
  - Volume: row counts within expected bounds
"""

import duckdb
import json
import os
from datetime import datetime
from config import DB_PATH, PROJECT_ROOT


# ---------- CHECK DEFINITIONS ----------
# Each check is a dict: {name, sql, pass_condition, severity}

BRONZE_CHECKS = [
    {
        "name": "bronze_insurance_row_count",
        "description": "Bronze insurance should have at least 5000 rows",
        "sql": "SELECT COUNT(*) FROM bronze_insurance",
        "pass_condition": lambda x: x >= 5000,
        "severity": "critical",
    },
    {
        "name": "bronze_employees_row_count",
        "description": "Bronze employees should have at least 500 rows",
        "sql": "SELECT COUNT(*) FROM bronze_employees",
        "pass_condition": lambda x: x >= 500,
        "severity": "critical",
    },
    {
        "name": "bronze_vendors_row_count",
        "description": "Bronze vendors should have at least 100 rows",
        "sql": "SELECT COUNT(*) FROM bronze_vendors",
        "pass_condition": lambda x: x >= 100,
        "severity": "critical",
    },
]


SILVER_CHECKS = [
    {
        "name": "silver_transaction_id_unique",
        "description": "TRANSACTION_ID must be unique in silver_insurance",
        "sql": """
            SELECT COUNT(*) - COUNT(DISTINCT TRANSACTION_ID)
            FROM silver_insurance
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "critical",
    },
    {
        "name": "silver_claim_amount_positive",
        "description": "CLAIM_AMOUNT must be positive",
        "sql": """
            SELECT COUNT(*) FROM silver_insurance
            WHERE CLAIM_AMOUNT <= 0
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "critical",
    },
    {
        "name": "silver_age_valid_range",
        "description": "AGE should be between 18 and 100",
        "sql": """
            SELECT COUNT(*) FROM silver_insurance
            WHERE AGE < 18 OR AGE > 100
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "warning",
    },
    {
        "name": "silver_claim_dates_valid",
        "description": "LOSS_DT should be <= REPORT_DT",
        "sql": """
            SELECT COUNT(*) FROM silver_insurance
            WHERE loss_date > report_date
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "critical",
    },
    {
        "name": "silver_agent_id_referential",
        "description": "All AGENT_IDs in claims must exist in silver_employees",
        "sql": """
            SELECT COUNT(*) FROM silver_insurance si
            LEFT JOIN silver_employees se ON si.AGENT_ID = se.AGENT_ID
            WHERE si.AGENT_ID IS NOT NULL AND se.AGENT_ID IS NULL
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "warning",
    },
    {
        "name": "silver_claim_amount_null_pct",
        "description": "CLAIM_AMOUNT should have 0% nulls in silver",
        "sql": """
            SELECT 100.0 * COUNT(CASE WHEN CLAIM_AMOUNT IS NULL THEN 1 END) / COUNT(*)
            FROM silver_insurance
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "critical",
    },
]


GOLD_CHECKS = [
    {
        "name": "gold_state_summary_nonempty",
        "description": "gold_state_risk_summary should have rows",
        "sql": "SELECT COUNT(*) FROM gold_state_risk_summary",
        "pass_condition": lambda x: x > 0,
        "severity": "critical",
    },
    {
        "name": "gold_total_matches_silver",
        "description": "Gold total claim amount should equal Silver's",
        "sql": """
            SELECT 
                ABS(
                  (SELECT SUM(total_claim_amount) FROM gold_state_risk_summary)
                  -
                  (SELECT SUM(CLAIM_AMOUNT) FROM silver_claims_enriched)
                )
        """,
        "pass_condition": lambda x: x < 1,  # allow for tiny rounding
        "severity": "critical",
    },
    {
        "name": "gold_agent_performance_valid",
        "description": "Every agent in gold should have at least 1 claim",
        "sql": """
            SELECT COUNT(*) FROM gold_agent_performance
            WHERE total_claims_processed < 1
        """,
        "pass_condition": lambda x: x == 0,
        "severity": "warning",
    },
]


# ---------- CHECK RUNNER ----------
def run_check(conn, check):
    """Execute a single check and return result."""
    try:
        value = conn.execute(check["sql"]).fetchone()[0]
        passed = check["pass_condition"](value)
        return {
            "name": check["name"],
            "description": check["description"],
            "severity": check["severity"],
            "value": value,
            "passed": passed,
            "status": "PASS" if passed else "FAIL",
        }
    except Exception as e:
        return {
            "name": check["name"],
            "description": check["description"],
            "severity": check["severity"],
            "value": None,
            "passed": False,
            "status": "ERROR",
            "error": str(e),
        }


def run_layer_checks(conn, layer_name, checks):
    """Run all checks for a layer, return results + summary."""
    print(f"\n{'='*60}")
    print(f"🔍 Running {layer_name.upper()} quality checks")
    print('='*60)

    results = []
    for check in checks:
        result = run_check(conn, check)
        results.append(result)

        # Visual output
        emoji = "✅" if result["status"] == "PASS" else (
            "⚠️ " if result["severity"] == "warning" else "❌"
        )
        print(f"  {emoji} [{result['severity']:8}] {result['name']}")
        print(f"     {result['description']}")
        print(f"     Value: {result['value']}  →  {result['status']}")

    # Summary
    passed = sum(1 for r in results if r["passed"])
    failed_critical = sum(
        1 for r in results 
        if not r["passed"] and r["severity"] == "critical"
    )
    failed_warning = sum(
        1 for r in results 
        if not r["passed"] and r["severity"] == "warning"
    )

    print(f"\n📊 {layer_name.upper()} Summary: "
          f"{passed} passed, {failed_critical} critical fails, "
          f"{failed_warning} warnings")

    return {
        "layer": layer_name,
        "passed": passed,
        "failed_critical": failed_critical,
        "failed_warning": failed_warning,
        "checks": results,
    }


def run_all_quality_checks():
    """Run all DQ checks and write report to disk."""
    conn = duckdb.connect(DB_PATH)

    report = {
        "timestamp": datetime.now().isoformat(),
        "layers": [
            run_layer_checks(conn, "bronze", BRONZE_CHECKS),
            run_layer_checks(conn, "silver", SILVER_CHECKS),
            run_layer_checks(conn, "gold",   GOLD_CHECKS),
        ],
    }

    # Aggregate summary
    total_passed = sum(l["passed"] for l in report["layers"])
    total_critical = sum(l["failed_critical"] for l in report["layers"])
    total_warning = sum(l["failed_warning"] for l in report["layers"])

    report["summary"] = {
        "total_checks": sum(len(l["checks"]) for l in report["layers"]),
        "total_passed": total_passed,
        "total_critical_failures": total_critical,
        "total_warnings": total_warning,
        "pipeline_healthy": total_critical == 0,
    }

    # Save report to JSON
    report_path = os.path.join(PROJECT_ROOT, "quality_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"📄 FINAL QUALITY REPORT")
    print('='*60)
    print(f"  Total checks:    {report['summary']['total_checks']}")
    print(f"  ✅ Passed:       {report['summary']['total_passed']}")
    print(f"  ❌ Critical:     {report['summary']['total_critical_failures']}")
    print(f"  ⚠️  Warnings:    {report['summary']['total_warnings']}")

    if report['summary']['pipeline_healthy']:
        print(f"\n  🎉 Pipeline is HEALTHY — safe to proceed!")
    else:
        print(f"\n  🚨 Pipeline has CRITICAL issues — review required!")

    print(f"\n📁 Report saved to: {report_path}\n")

    conn.close()
    return report


if __name__ == "__main__":
    run_all_quality_checks()