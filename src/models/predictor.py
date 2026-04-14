"""Load saved models and predict EC price_per_sqm from raw URA-style input."""

from __future__ import annotations

import logging

import joblib
import pandas as pd

from src.config import MODELS_DIR
from src.features.preprocessing import (
    _load_town_mapping,
    _load_town_to_region,
    _get_town,
    _smart_remaining_lease,
    build_sklearn_pipeline,
)
from src.models.trainer import MOP_REMAINING_LEASE, PRIVATISED_REMAINING_LEASE

logger = logging.getLogger(__name__)

_cache: dict = {}


def _load_artefacts() -> None:
    """Load models + pipeline into module-level cache (once)."""
    if _cache:
        return

    pipeline_path = MODELS_DIR / "preprocessing_pipeline.joblib"
    if not pipeline_path.exists():
        raise RuntimeError("Models not trained yet. Call POST /train first.")

    _cache["pipeline"] = joblib.load(pipeline_path)
    _cache["rf_94"] = joblib.load(MODELS_DIR / "rf_lease_94.joblib")
    _cache["rfe_94"] = joblib.load(MODELS_DIR / "rfe_lease_94.joblib")
    _cache["rf_89"] = joblib.load(MODELS_DIR / "rf_lease_89.joblib")
    _cache["rfe_89"] = joblib.load(MODELS_DIR / "rfe_lease_89.joblib")
    _cache["town_mapping"] = _load_town_mapping()
    _cache["town_to_region"] = _load_town_to_region()
    logger.info("Model artefacts loaded from %s", MODELS_DIR)


def reload_models() -> None:
    """Force-reload artefacts (e.g. after re-training)."""
    _cache.clear()
    _load_artefacts()


def _raw_to_features(raw: dict) -> dict:
    """Convert raw URA-style fields into the feature dict the pipeline expects."""
    contract_date = str(raw.get("contractDate", "0101")).zfill(4)
    contract_month = int(contract_date[:2])
    contract_year = int(contract_date[2:])

    floor_range = str(raw.get("floorRange", "01-05"))
    import re
    lo = re.search(r"(\d+)", floor_range)
    hi = re.search(r"-(\d+)", floor_range)
    higher_floor_range = int(hi.group(1)) if hi else (int(lo.group(1)) if lo else 5)

    remaining_lease = float(_smart_remaining_lease(raw.get("tenure", "")))

    street_upper = str(raw.get("street", "")).strip().upper()
    town = _get_town(street_upper, _cache["town_mapping"])
    region_raw = _cache["town_to_region"].get(town, "") if town else ""
    region = region_raw.replace(" REGION", "") if region_raw else None

    return {
        "typeOfSale": int(raw.get("typeOfSale", 3)),
        "district": int(raw.get("district", 1)),
        "contractMonth": contract_month,
        "contractYear": contract_year,
        "higher_floor_range": higher_floor_range,
        "propertyType": raw.get("propertyType", "Executive Condominium"),
        "typeOfArea": raw.get("typeOfArea", "Strata"),
        "marketSegment": raw.get("marketSegment", "OCR"),
        "town": town,
        "region": region,
        "remaining_lease": remaining_lease,
        "price_per_sqm": 0.0,
    }, town, region, remaining_lease


def predict(raw_features: dict) -> dict:
    """Predict ``price_per_sqm`` from raw URA-style input fields.

    Accepts: area, floorRange, noOfUnits, contractDate, typeOfSale,
             propertyType, district, typeOfArea, tenure, street, marketSegment.

    Derives: contractMonth/Year, higher_floor_range, remaining_lease,
             town, region — then selects the correct model automatically.
    """
    _load_artefacts()

    features, town, region, remaining_lease = _raw_to_features(raw_features)

    if remaining_lease == MOP_REMAINING_LEASE:
        rf = _cache["rf_94"]
        rfe = _cache["rfe_94"]
        label = "Lease 94 (MOP)"
    elif remaining_lease == PRIVATISED_REMAINING_LEASE:
        rf = _cache["rf_89"]
        rfe = _cache["rfe_89"]
        label = "Lease 89 (Privatised)"
    else:
        raise ValueError(
            f"Computed remaining_lease = {remaining_lease} from tenure "
            f"'{raw_features.get('tenure')}'. "
            f"Only {MOP_REMAINING_LEASE} (MOP) or {PRIVATISED_REMAINING_LEASE} "
            f"(Privatised) are supported."
        )

    from sklearn import set_config
    set_config(transform_output="pandas")

    df_input = pd.DataFrame([features])
    df_transformed = _cache["pipeline"].transform(df_input)

    drop_cols = [
        c for c in df_transformed.columns
        if "price_per_sqm" in c or "remaining_lease" in c
    ]
    X = df_transformed.drop(columns=drop_cols).astype(float)
    X_sel = rfe.transform(X)
    prediction = float(rf.predict(X_sel)[0])

    return {
        "model_used": label,
        "remaining_lease": float(remaining_lease),
        "town": town,
        "region": region,
        "predicted_price_per_sqm": round(prediction, 2),
    }
