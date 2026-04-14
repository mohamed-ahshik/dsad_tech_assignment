from __future__ import annotations

import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

URA_KEY: str = os.environ.get("URA_KEY", "")
URA_TOKEN: str = os.environ.get("URA_TOKEN", "")

# Postgres connection — used by docker-compose (service name "db") and local dev
POSTGRES_DSN: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/ec_prices",
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = PROJECT_ROOT / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

GEOJSON_PATH = DATA_DIR / "MasterPlan2025PlanningAreaBoundaryNoSea.geojson"
HDB_RESALE_CSV = DATA_DIR / "Resale flat prices based on registration date from Jan-2017 onwards.csv"

URA_BASE_URL = "https://eservice.ura.gov.sg/uraDataService"
URA_TOKEN_URL = f"{URA_BASE_URL}/insertNewToken/v1"
URA_DATA_URL = f"{URA_BASE_URL}/invokeUraDS/v1"
URA_SERVICE = "PMI_Resi_Transaction"
URA_BATCHES = range(1, 5)
