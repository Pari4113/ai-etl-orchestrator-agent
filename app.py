"""
Streamlit UI for the AI ETL Orchestrator Agent.
Gives users a natural language interface to run the pipeline.
"""

import streamlit as st
import json
import os
from io import StringIO
import sys

from agents.orchestrator import plan_steps, execute_steps, summarize_run
from config import PROJECT_ROOT, DB_PATH
import duckdb


# ---------- PAGE CONFIG ----------
st.set_page_config(
    page_title="AI ETL Orchestrator",
    page_icon="🤖",
    layout="wide",
)


# ---------- HEADER ----------
st.title("🤖 AI ETL Orchestrator Agent")
st.markdown("""
Run the insurance claims ETL pipeline via natural language.  
Built with **LangChain + Groq (Llama 3.3 70B)** • **DuckDB** • **Medallion Architecture** (Bronze → Silver → Gold)
""")
st.divider()


# ---------- SIDEBAR: PROJECT INFO ----------
with st.sidebar:
    st.header("📊 Project Info")
    st.markdown("""
    **Tech Stack:**
    - 🧠 Groq LLM for planning & summaries
    - 🗄️ DuckDB local warehouse
    - 🏗️ Medallion Architecture
    - ✅ Automated Data Quality Checks
    - 🩹 Self-healing error diagnosis
    """)

    st.divider()

    st.header("🗂️ Warehouse")
    if os.path.exists(DB_PATH):
        conn = duckdb.connect(DB_PATH)
        tables = conn.execute("SHOW TABLES").fetchall()
        st.write(f"**{len(tables)} tables** in warehouse:")
        for t in tables:
            st.markdown(f"- `{t[0]}`")
        conn.close()
    else:
        st.info("Warehouse not initialized yet. Run the pipeline.")

    st.divider()
    st.caption("GitHub: [Pari4113/ai-etl-orchestrator-agent](https://github.com/Pari4113/ai-etl-orchestrator-agent)")


# ---------- MAIN: INSTRUCTION INPUT ----------
st.subheader("💬 Give the Agent an Instruction")

col1, col2 = st.columns([3, 1])
with col1:
    user_instruction = st.text_input(
        "What would you like the pipeline agent to do?",
        placeholder="e.g. Run the whole pipeline and verify data quality",
        label_visibility="collapsed",
    )
with col2:
    run_button = st.button("▶️ Run Agent", type="primary", use_container_width=True)


# Example prompts
st.caption("💡 Try:")
example_prompts = [
    "Run the whole pipeline and verify data quality",
    "Just load the raw data",
    "Rebuild the gold layer",
    "Check data quality only",
]
cols = st.columns(len(example_prompts))
for i, prompt in enumerate(example_prompts):
    if cols[i].button(prompt, key=f"ex{i}"):
        user_instruction = prompt
        run_button = True


# ---------- RUN THE AGENT ----------
if run_button and user_instruction:
    st.divider()

    # Step 1: Planning
    with st.spinner("🧠 AI is planning the pipeline steps..."):
        steps = plan_steps(user_instruction)
    st.success(f"**Planned steps:** {' → '.join(steps)}")

    # Step 2: Execution
    st.subheader("⚙️ Execution Log")
    log_container = st.container()
    with log_container:
        # Capture stdout so we can show what's happening
        old_stdout = sys.stdout
        sys.stdout = captured = StringIO()

        try:
            with st.spinner("🚀 Running the pipeline..."):
                results = execute_steps(steps)
        finally:
            sys.stdout = old_stdout

        # Show the captured log
        st.code(captured.getvalue(), language="text")

    # Step 3: Summary
    with st.spinner("📝 Generating plain English summary..."):
        summary = summarize_run(user_instruction, steps, results)

    st.subheader("📋 Agent Summary")
    st.info(summary)

    # Step 4: Show Quality Report (if available)
    quality_report_path = os.path.join(PROJECT_ROOT, "quality_report.json")
    if os.path.exists(quality_report_path) and "quality" in results:
        st.divider()
        st.subheader("📊 Data Quality Report")

        with open(quality_report_path) as f:
            report = json.load(f)

        s = report["summary"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Checks", s["total_checks"])
        c2.metric("✅ Passed", s["total_passed"])
        c3.metric("❌ Critical", s["total_critical_failures"])
        c4.metric("⚠️ Warnings", s["total_warnings"])

        if s["pipeline_healthy"]:
            st.success("🎉 Pipeline is HEALTHY")
        else:
            st.error("🚨 Pipeline has CRITICAL issues — review required")

        # Detailed view per layer
        with st.expander("🔍 See detailed checks"):
            for layer in report["layers"]:
                st.markdown(f"### {layer['layer'].upper()} Layer")
                for check in layer["checks"]:
                    status_emoji = "✅" if check["passed"] else (
                        "⚠️" if check["severity"] == "warning" else "❌"
                    )
                    st.markdown(
                        f"{status_emoji} **{check['name']}** "
                        f"(`{check['severity']}`) — {check['description']}  "
                        f"\n   Value: `{check['value']}` → **{check['status']}**"
                    )

elif run_button and not user_instruction:
    st.warning("Please enter an instruction first.")