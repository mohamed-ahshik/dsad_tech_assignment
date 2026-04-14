"""Feature engineering pipeline — mirrors the EDA notebook logic."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder

from src.config import GEOJSON_PATH, HDB_RESALE_CSV

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CURRENT_YEAR = datetime.today().year

SCALING_COLS = [
    "typeOfSale",
    "district",
    "contractMonth",
    "contractYear",
    "higher_floor_range",
]

OHE_COLS = ["propertyType", "typeOfArea", "marketSegment", "town", "region"]

# Street-name abbreviation map for town matching
_ABBREV_MAP = {
    "ST": "STREET", "AVE": "AVENUE", "RD": "ROAD", "DR": "DRIVE",
    "CRES": "CRESCENT", "CTRL": "CENTRAL", "NTH": "NORTH", "STH": "SOUTH",
    "UPP": "UPPER", "BT": "BUKIT", "JLN": "JALAN", "LOR": "LORONG",
    "TER": "TERRACE", "CL": "CLOSE", "PL": "PLACE", "PK": "PARK",
    "GDNS": "GARDENS", "HTS": "HEIGHTS", "KG": "KAMPONG", "MKT": "MARKET",
    "TG": "TANJONG", "C'WEALTH": "COMMONWEALTH",
}

# Keyword → town fallback (most-specific first)
_TOWN_KEYWORDS: list[tuple[str, str]] = [
    ("CHOA CHU KANG", "CHOA CHU KANG"),
    ("BUKIT BATOK", "BUKIT BATOK"),
    ("BUKIT MERAH", "BUKIT MERAH"),
    ("BUKIT PANJANG", "BUKIT PANJANG"),
    ("ANG MO KIO", "ANG MO KIO"),
    ("JURONG EAST", "JURONG EAST"),
    ("JURONG WEST", "JURONG WEST"),
    ("MARINE PARADE", "MARINE PARADE"),
    ("TOA PAYOH", "TOA PAYOH"),
    ("YIO CHU KANG", "ANG MO KIO"),
    ("JALAN LOYANG", "PASIR RIS"),
    ("PLANTATION", "CHOA CHU KANG"),
    ("WESTWOOD", "JURONG WEST"),
    ("CANBERRA", "SEMBAWANG"),
    ("FERNVALE", "SENGKANG"),
    ("RIVERVALE", "SENGKANG"),
    ("ANCHORVALE", "SENGKANG"),
    ("COMPASSVALE", "SENGKANG"),
    ("SUMANG", "PUNGGOL"),
    ("TENGAH", "TENGAH"),
    ("PASIR RIS", "PASIR RIS"),
    ("SENGKANG", "SENGKANG"),
    ("PUNGGOL", "PUNGGOL"),
    ("TAMPINES", "TAMPINES"),
    ("SIMEI", "TAMPINES"),
    ("WOODLANDS", "WOODLANDS"),
    ("SEMBAWANG", "SEMBAWANG"),
    ("YISHUN", "YISHUN"),
    ("HOUGANG", "HOUGANG"),
    ("SERANGOON", "SERANGOON"),
    ("BISHAN", "BISHAN"),
    ("CLEMENTI", "CLEMENTI"),
    ("BEDOK", "BEDOK"),
    ("GEYLANG", "GEYLANG"),
    ("QUEENSTOWN", "QUEENSTOWN"),
    ("KALLANG", "KALLANG/WHAMPOA"),
    ("WHAMPOA", "KALLANG/WHAMPOA"),
    ("BOON LAY", "JURONG WEST"),
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _expand_street_abbrevs(name: str) -> str:
    for abbr, full in _ABBREV_MAP.items():
        name = re.sub(rf"\b{re.escape(abbr)}\b", full, name)
    return name


def _smart_remaining_lease(tenure: Any) -> float:
    if pd.isnull(tenure):
        return np.nan
    t = str(tenure).lower().strip()

    if "freehold" in t:
        return 999

    if re.search(r"999[\s\-]*year", t) or re.search(r"999[\s\-]*yrs", t):
        return 999

    match_99 = re.search(r"99[\s\-]*yrs* lease.*?from (\d{4})", t)
    if match_99:
        return max(0, 99 - (CURRENT_YEAR - int(match_99.group(1))))

    match_nnn = re.search(r"(\d{2,4})[\s\-]*yrs* lease.*?from (\d{4})", t)
    if match_nnn:
        nnn = int(match_nnn.group(1))
        return max(0, nnn - (CURRENT_YEAR - int(match_nnn.group(2))))

    # Fallback: plain number of years
    match_plain = re.search(r"(\d{2,4})[\s\-]*(year|yrs)", t)
    if match_plain:
        return int(match_plain.group(1))

    return np.nan


def _load_town_mapping() -> dict[str, str]:
    """Build street→town dict from HDB resale CSV."""
    if not HDB_RESALE_CSV.exists():
        return {}
    mapping_df = pd.read_csv(HDB_RESALE_CSV, usecols=["street_name", "town"])
    mapping_df["street_name"] = (
        mapping_df["street_name"].str.strip().str.upper().apply(_expand_street_abbrevs)
    )
    return (
        mapping_df.drop_duplicates("street_name")
        .set_index("street_name")["town"]
        .to_dict()
    )


def _load_town_to_region() -> dict[str, str]:
    """Build town→region dict from the MasterPlan GeoJSON."""
    if not GEOJSON_PATH.exists():
        return {}
    with open(GEOJSON_PATH) as f:
        gj = json.load(f)
    return {
        feat["properties"]["PLN_AREA_N"]: feat["properties"]["REGION_N"]
        for feat in gj["features"]
        if feat["properties"].get("REGION_N")
    }


def _get_town(street: str, mapping_dict: dict[str, str]) -> str | None:
    town = mapping_dict.get(street)
    if town:
        return town
    for keyword, t in _TOWN_KEYWORDS:
        if keyword in street:
            return t
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_from_supabase() -> pd.DataFrame:
    """Read all property transactions from Supabase and return a raw-ish
    DataFrame in the same shape the EDA notebook produces after
    ``json_normalize``.
    """
    from src.database.client import fetch_ec_transactions

    rows = fetch_ec_transactions()
    df = pd.DataFrame(rows)

    rename_map = {
        "property_type": "propertyType",
        "type_of_sale": "typeOfSale",
        "no_of_units": "noOfUnits",
        "nett_price": "nettPrice",
        "type_of_area": "typeOfArea",
        "floor_range": "floorRange",
        "contract_date": "contractDate",
        "market_segment": "marketSegment",
    }
    df = df.rename(columns=rename_map)

    return df


def build_ec_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Clean and feature-engineer the raw transaction DataFrame.

    Mirrors EDA cells 28–57 (filter ECs, split floor range, derive
    remaining_lease, map town/region, derive price_per_sqm, drop unneeded
    columns).
    """
    df = df_raw.copy()

    # Ensure contractMonth / contractYear exist
    if "contractMonth" not in df.columns:
        df["contractDate"] = df["contractDate"].astype(str).str.zfill(4)
        df["contractMonth"] = df["contractDate"].str[:2].astype(int)
        df["contractYear"] = df["contractDate"].str[2:].astype(int)

    # Filter to ECs
    df = df[df["propertyType"] == "Executive Condominium"].copy()
    if df.empty:
        raise RuntimeError("No Executive Condominium rows in the data.")

    # Cast core columns
    for col, dtype in [
        ("area", float), ("price", float), ("noOfUnits", int),
        ("typeOfSale", int), ("district", int),
        ("contractMonth", int), ("contractYear", int),
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Floor range → lower / higher
    df["lower_floor_range"] = df["floorRange"].str.extract(r"(\d+)").astype(float)
    df["higher_floor_range"] = df["floorRange"].str.extract(r"-(\d+)").astype(float)

    # Derived columns
    df["price_per_sqm"] = df["price"] / df["area"]
    df["remaining_lease"] = df["tenure"].apply(_smart_remaining_lease)

    # Town mapping
    mapping_dict = _load_town_mapping()
    df["street_upper"] = df["street"].fillna("").str.strip().str.upper()
    df["town"] = df["street_upper"].apply(lambda s: _get_town(s, mapping_dict))

    # Region mapping
    town_to_region = _load_town_to_region()
    df["region"] = (
        df["town"]
        .map(town_to_region)
        .str.replace(" REGION", "", regex=False)
    )

    # Drop columns not needed for modelling
    drop_cols = [
        "id", "property_id", "property_id_join", "created_at", "created_at_prop",
        "nettPrice", "x", "y", "project", "tenure", "street", "street_upper",
        "floorRange", "contractDate", "area", "price", "noOfUnits",
        "lower_floor_range",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
    df = df.drop_duplicates()

    return df


def build_sklearn_pipeline() -> Pipeline:
    """Return the sklearn ColumnTransformer pipeline (unfitted).

    Numeric columns are MinMax-scaled; categorical columns are one-hot
    encoded. ``remainder='passthrough'`` keeps price_per_sqm and
    remaining_lease for later splitting.
    """
    num_pipe = Pipeline([("min_max_scaler", MinMaxScaler())])
    cat_pipe = Pipeline([("onehot", OneHotEncoder(sparse_output=False, handle_unknown="ignore"))])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", num_pipe, SCALING_COLS),
            ("cat", cat_pipe, OHE_COLS),
        ],
        remainder="passthrough",
    )

    return Pipeline([("column_preprocessor", preprocessor)])
