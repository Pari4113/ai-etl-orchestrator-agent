"""
AI ETL Orchestrator: The brain of the pipeline.

Uses Groq LLM to:
  1. Plan which pipeline steps to run based on user instruction
  2. Execute the selected steps
  3. Read quality report and generate a plain English summary
"""

import json
from agents.healer import run_with_healing
from langchain_groq import ChatGroq
from config import GROQ_API_KEY
from agents.healer import run_with_healing

# Import our pipeline functions
from agents.bronze_loader import load_to_bronze
from agents.silver_cleaner import build_silver_layer
from agents.gold_builder import build_gold_layer
from agents.quality_checker import run_all_quality_checks


# ---------- Set up the LLM ----------
llm = ChatGroq(
    api_key=GROQ_API_KEY,
    model="llama-3.3-70b-versatile",
    temperature=0,  # deterministic, no randomness for planning
)


# ---------- Available pipeline steps ----------
AVAILABLE_STEPS = {
    "bronze":  load_to_bronze,
    "silver":  build_silver_layer,
    "gold":    build_gold_layer,
    "quality": run_all_quality_checks,
}


# ---------- The Planner ----------
def plan_steps(user_instruction: str) -> list:
    """
    Uses the LLM to decide which pipeline steps to run
    based on the user's natural language instruction.
    """

    planning_prompt = f"""
You are an ETL pipeline orchestrator. Your job is to decide 
which pipeline steps to run based on the user's request.

Available steps (in order):
  - bronze:  Load raw CSV data into the warehouse (Bronze layer)
  - silver:  Clean and join the data (Silver layer)
  - gold:    Build aggregated business tables (Gold layer)
  - quality: Run data quality checks on all layers

Rules:
  - Steps have dependencies: silver needs bronze, gold needs silver, 
    quality needs all layers to exist.
  - Return ONLY a JSON list of step names, nothing else.
  - If unsure, run all steps in order.

Examples:
  User: "Run the whole pipeline"
  Response: ["bronze", "silver", "gold", "quality"]

  User: "Just load raw data"
  Response: ["bronze"]

  User: "Rebuild everything and check quality"
  Response: ["bronze", "silver", "gold", "quality"]

  User: "Check data quality"
  Response: ["quality"]

User request: {user_instruction}

Response (JSON list only):
""".strip()

    response = llm.invoke(planning_prompt)
    content = response.content.strip()

    # Extract the JSON list from the response
    try:
        # LLM sometimes wraps in code blocks; clean that
        content = content.replace("```json", "").replace("```", "").strip()
        steps = json.loads(content)
        return steps
    except json.JSONDecodeError:
        print(f"⚠️  Could not parse LLM response: {content}")
        print("   Falling back to full pipeline run.")
        return ["bronze", "silver", "gold", "quality"]


# ---------- The Executor ----------
def execute_steps(steps: list) -> dict:
    """Runs the selected pipeline steps with AI-assisted self-healing."""
    results = {}
    for step in steps:
        if step not in AVAILABLE_STEPS:
            print(f"⚠️  Unknown step: {step}, skipping")
            continue

        result = run_with_healing(step, AVAILABLE_STEPS[step])
        results[step] = result

        # Stop pipeline if a step ultimately failed
        if result["status"] == "failed":
            print(f"\n🛑 Stopping pipeline — '{step}' could not be healed.")
            break
    return results


# ---------- The Reporter ----------
def summarize_run(user_instruction: str, steps: list, results: dict) -> str:
    """Uses the LLM to write a plain English summary of what happened."""

    # Read the quality report if it exists
    quality_summary = "No quality check was run."
    if "quality" in results and results["quality"]["status"] == "success":
        try:
            with open("quality_report.json") as f:
                report = json.load(f)
            s = report["summary"]
            quality_summary = (
                f"Ran {s['total_checks']} quality checks: "
                f"{s['total_passed']} passed, "
                f"{s['total_critical_failures']} critical failures, "
                f"{s['total_warnings']} warnings. "
                f"Pipeline healthy: {s['pipeline_healthy']}"
            )
        except Exception:
            quality_summary = "Quality report could not be read."

    report_prompt = f"""
You are a data engineer writing a short, professional 
summary of an ETL pipeline run.

The user asked: "{user_instruction}"

Steps executed: {steps}
Results per step: {json.dumps(results, default=str)}
Quality summary: {quality_summary}

Write a clear, concise summary (3-5 sentences) of:
  - What ran
  - Whether it succeeded
  - Quality status
  - Any issues to be aware of

Do not use markdown headers or bullet points. 
Write like a teammate reporting back, in plain prose.
""".strip()

    response = llm.invoke(report_prompt)
    return response.content.strip()


# ---------- The Orchestrator ----------
def run_agent(user_instruction: str):
    """End-to-end agent: plan → execute → summarize."""

    print(f"\n{'='*60}")
    print(f"🤖 AI ETL ORCHESTRATOR")
    print('='*60)
    print(f"\n📝 User request: {user_instruction}")

    # Step 1: Plan
    print(f"\n🧠 Planning steps...")
    steps = plan_steps(user_instruction)
    print(f"   Steps chosen: {steps}")

    # Step 2: Execute
    print(f"\n⚙️  Executing pipeline...")
    results = execute_steps(steps)

    # Step 3: Summarize
    print(f"\n📝 Generating summary...")
    summary = summarize_run(user_instruction, steps, results)

    print(f"\n{'='*60}")
    print(f"📋 RUN SUMMARY")
    print('='*60)
    print(f"\n{summary}\n")
    print('='*60)
    return summary


# ---------- Run as script ----------
if __name__ == "__main__":
    # Test with a plain English instruction
    user_prompt = "Run the whole pipeline and verify the data quality"
    run_agent(user_prompt)