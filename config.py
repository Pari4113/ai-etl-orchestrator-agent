"""
Central configuration for the ETL Agent project.
Keeps all paths and settings in one place.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ---------- API KEYS ----------
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# ---------- PROJECT PATHS ----------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
WAREHOUSE_DIR = os.path.join(PROJECT_ROOT, "warehouse")
DB_PATH = os.path.join(WAREHOUSE_DIR, "insurance_warehouse.duckdb")

# ---------- SOURCE FILES ----------
SOURCE_FILES = {
    "insurance": os.path.join(DATA_DIR, "insurance_data.csv"),
    "employees": os.path.join(DATA_DIR, "employee_data.csv"),
    "vendors":   os.path.join(DATA_DIR, "vendor_data.csv"),
}

# ---------- BRONZE TABLE NAMES ----------
BRONZE_TABLES = {
    "insurance": "bronze_insurance",
    "employees": "bronze_employees",
    "vendors":   "bronze_vendors",
}