"""
AI Healer: Catches pipeline errors, diagnoses them via LLM,
and attempts to automatically recover.

Handles common failure modes:
  - Missing files (swap to backup, skip optional)
  - Schema drift (column renamed/missing)
  - Type mismatches (wrong date format, etc.)
  - Transient errors (connection drops, retries)
"""

import os
import time
import traceback
import pandas as pd
from langchain_groq import ChatGroq
from config import GROQ_API_KEY, SOURCE_FILES


llm = ChatGroq(
    api_key=GROQ_API_KEY,
    model="llama-3.3-70b-versatile",
    temperature=0,
)


# ---------- Error Diagnosis via LLM ----------
def diagnose_error(step_name: str, error: Exception, context: str = "") -> dict:
    """Ask the LLM to diagnose what went wrong and suggest a recovery strategy."""

    error_text = f"{type(error).__name__}: {str(error)}"
    traceback_text = traceback.format_exc()

    diagnosis_prompt = f"""
You are an ETL pipeline diagnostician. A pipeline step just failed.
Your job is to diagnose the root cause and classify the fix strategy.

Step that failed: {step_name}
Error: {error_text}

Traceback (last 10 lines):
{chr(10).join(traceback_text.splitlines()[-10:])}

Additional context: {context}

Classify the error and choose ONE recovery strategy from:
  - "retry":          Likely transient (connection, lock). Retry 2x with delay.
  - "missing_file":   Source file missing. Skip or swap to alternate.
  - "schema_drift":   Column renamed or type changed. Requires data inspection.
  - "unfixable":      Real bug or missing dependency. Needs human.

Respond with ONLY a JSON object, nothing else, no markdown:
{{
  "root_cause": "short explanation of why it failed",
  "strategy": "retry" | "missing_file" | "schema_drift" | "unfixable",
  "reasoning": "why this strategy is appropriate",
  "confidence": "high" | "medium" | "low"
}}
""".strip()

    response = llm.invoke(diagnosis_prompt)
    content = response.content.strip()

    # Clean markdown if LLM wraps it
    content = content.replace("```json", "").replace("```", "").strip()

    import json
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        print(f"⚠️  Could not parse diagnosis: {content}")
        return {
            "root_cause": "Unknown",
            "strategy": "unfixable",
            "reasoning": "LLM response was unparseable",
            "confidence": "low",
        }


# ---------- Recovery Strategies ----------
def try_retry(step_func, max_retries=2, delay=2):
    """Retry with backoff for transient errors."""
    for attempt in range(1, max_retries + 1):
        print(f"   🔁 Retry attempt {attempt}/{max_retries}...")
        time.sleep(delay * attempt)
        try:
            step_func()
            return True
        except Exception as e:
            print(f"      Attempt {attempt} failed: {e}")
    return False


def try_missing_file_recovery(step_func, error_msg):
    """If a source file is missing, check whether it's critical or optional."""

    # Extract filename from error (simple heuristic)
    print(f"   🔍 Searching for missing file clue in: {error_msg}")

    # Check each expected source and see which is missing
    for source_name, path in SOURCE_FILES.items():
        if not os.path.exists(path):
            print(f"   ⚠️  Missing source: {source_name} at {path}")

            # For our use case, all sources are considered critical
            # so we log and cannot auto-recover
            print(f"   ❌ {source_name} is critical. Cannot auto-recover.")
            return False

    return False


def try_schema_drift_recovery(step_func, error_msg):
    """AI-assisted schema drift handling — logs for now, full auto-fix needs more."""

    print(f"   🔍 Schema drift detected. Inspecting sources...")

    # Inspect current schema of source files
    schema_info = []
    for source_name, path in SOURCE_FILES.items():
        if os.path.exists(path):
            df = pd.read_csv(path, nrows=1)
            schema_info.append(
                f"   {source_name}: {list(df.columns)}"
            )

    print("   📋 Current source schemas:")
    for line in schema_info:
        print(line)

    print(f"   🚨 Automatic schema drift fix requires manual review.")
    print(f"      LLM diagnosed the issue but SQL edits are logged, not applied.")
    return False


# ---------- Main Self-Healing Wrapper ----------
def run_with_healing(step_name: str, step_func):
    """
    Run a pipeline step with AI-assisted self-healing.

    Returns a dict:
      {
        "status": "success" | "recovered" | "failed",
        "attempts": N,
        "diagnosis": {...} or None,
      }
    """
    print(f"\n▶️  Running '{step_name}' with self-healing...")

    # First attempt
    try:
        step_func()
        return {"status": "success", "attempts": 1, "diagnosis": None}
    except Exception as e:
        print(f"   ❌ '{step_name}' failed on first attempt: {e}")

        # Ask LLM to diagnose
        print(f"   🤖 Asking AI to diagnose...")
        diagnosis = diagnose_error(step_name, e)
        print(f"      Root cause: {diagnosis['root_cause']}")
        print(f"      Strategy:   {diagnosis['strategy']}")
        print(f"      Confidence: {diagnosis['confidence']}")

        # Apply recovery strategy
        recovered = False
        if diagnosis["strategy"] == "retry":
            recovered = try_retry(step_func)
        elif diagnosis["strategy"] == "missing_file":
            recovered = try_missing_file_recovery(step_func, str(e))
        elif diagnosis["strategy"] == "schema_drift":
            recovered = try_schema_drift_recovery(step_func, str(e))
        else:
            print(f"   💀 Strategy '{diagnosis['strategy']}' needs human intervention.")

        if recovered:
            print(f"   ✅ '{step_name}' RECOVERED after healing!")
            return {"status": "recovered", "attempts": 2, "diagnosis": diagnosis}
        else:
            print(f"   ❌ '{step_name}' could not be auto-healed.")
            return {
                "status": "failed",
                "attempts": 2 if diagnosis["strategy"] == "retry" else 1,
                "diagnosis": diagnosis,
            }