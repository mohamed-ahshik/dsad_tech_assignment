"""Train EC price prediction models for both lifecycle stages."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import joblib
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import RFECV
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from src.config import MODELS_DIR
from src.features.preprocessing import (
    build_ec_dataframe,
    build_sklearn_pipeline,
    load_from_supabase,
)

logger = logging.getLogger(__name__)

MOP_REMAINING_LEASE = 94
PRIVATISED_REMAINING_LEASE = 89


@dataclass
class ModelResult:
    label: str
    n_rows: int
    n_features_selected: int
    selected_features: list[str]
    rmse: float
    r2: float


def _train_single(
    df_processed: "pandas.DataFrame",
    label: str,
) -> tuple[RandomForestRegressor, RFECV, Pipeline, list[str], ModelResult]:
    """Train a single model on a pre-processed DataFrame.

    Returns (fitted_rf, fitted_rfe, fitted_pipeline, selected_features, result).
    """
    import pandas as pd

    target_col = [c for c in df_processed.columns if "price_per_sqm" in c][0]
    drop_cols = [
        c for c in df_processed.columns
        if "price_per_sqm" in c or "remaining_lease" in c
    ]

    X = df_processed.drop(columns=drop_cols).astype(float)
    y = df_processed[target_col].astype(float)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
    )

    clf = RandomForestRegressor(random_state=42, n_jobs=-1)
    rfe = RFECV(clf, cv=5, scoring="r2", n_jobs=-1)
    rfe.fit(X_train, y_train)

    selected_features = X.columns[rfe.support_].tolist()

    X_train_sel = rfe.transform(X_train)
    X_test_sel = rfe.transform(X_test)

    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train_sel, y_train)
    y_pred = rf.predict(X_test_sel)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = float(r2_score(y_test, y_pred))

    result = ModelResult(
        label=label,
        n_rows=int(len(df_processed)),
        n_features_selected=int(rfe.n_features_),
        selected_features=selected_features,
        rmse=rmse,
        r2=r2,
    )
    logger.info("[%s] Selected %d features: %s", label, rfe.n_features_, selected_features)
    logger.info("[%s] RMSE: %.2f  |  R²: %.4f", label, rmse, r2)

    return rf, rfe, None, selected_features, result


def train_models() -> dict:
    """Full training pipeline: load data → preprocess → train both models.

    Saves artefacts to ``models/`` and returns a summary dict.
    """
    from sklearn import set_config

    set_config(transform_output="pandas")

    df_raw = load_from_supabase()
    df_ec = build_ec_dataframe(df_raw)

    df_lease_94 = df_ec[df_ec["remaining_lease"] == MOP_REMAINING_LEASE].copy()
    df_lease_89 = df_ec[df_ec["remaining_lease"] == PRIVATISED_REMAINING_LEASE].copy()

    if df_lease_94.empty:
        raise RuntimeError(f"No rows with remaining_lease == {MOP_REMAINING_LEASE}")
    if df_lease_89.empty:
        raise RuntimeError(f"No rows with remaining_lease == {PRIVATISED_REMAINING_LEASE}")

    # Fit pipeline on ALL ECs so both subsets share the same feature space
    pipeline = build_sklearn_pipeline()
    pipeline.fit(df_ec)

    df_94_processed = pipeline.transform(df_lease_94)
    df_89_processed = pipeline.transform(df_lease_89)

    logger.info("Lease 94 processed shape: %s", df_94_processed.shape)
    logger.info("Lease 89 processed shape: %s", df_89_processed.shape)

    rf_94, rfe_94, _, feat_94, res_94 = _train_single(df_94_processed, "Lease 94 (MOP)")
    rf_89, rfe_89, _, feat_89, res_89 = _train_single(df_89_processed, "Lease 89 (Privatised)")

    # Persist artefacts
    joblib.dump(pipeline, MODELS_DIR / "preprocessing_pipeline.joblib")
    joblib.dump(rf_94, MODELS_DIR / "rf_lease_94.joblib")
    joblib.dump(rfe_94, MODELS_DIR / "rfe_lease_94.joblib")
    joblib.dump(rf_89, MODELS_DIR / "rf_lease_89.joblib")
    joblib.dump(rfe_89, MODELS_DIR / "rfe_lease_89.joblib")

    logger.info("Model artefacts saved to %s", MODELS_DIR)

    return {
        "lease_94_mop": {
            "rows": int(res_94.n_rows),
            "features_selected": int(res_94.n_features_selected),
            "selected_features": [str(f) for f in res_94.selected_features],
            "rmse": round(float(res_94.rmse), 2),
            "r2": round(float(res_94.r2), 4),
        },
        "lease_89_privatised": {
            "rows": int(res_89.n_rows),
            "features_selected": int(res_89.n_features_selected),
            "selected_features": [str(f) for f in res_89.selected_features],
            "rmse": round(float(res_89.rmse), 2),
            "r2": round(float(res_89.r2), 4),
        },
    }


def _log_line(msg: str) -> str:
    """Encode a log line as newline-delimited JSON for SSE streaming."""
    return json.dumps({"type": "log", "message": msg}) + "\n"


def train_models_iter() -> Generator[str, None, None]:
    """Generator version of train_models — yields progress log lines then
    a final ``result`` JSON line.  Consumed by the ``/train/stream`` endpoint.
    """
    from sklearn import set_config

    set_config(transform_output="pandas")

    yield _log_line("📥 Loading EC transactions from database...")
    df_raw = load_from_supabase()

    yield _log_line(f"🔧 Building feature matrix ({len(df_raw):,} raw rows)...")
    df_ec = build_ec_dataframe(df_raw)

    df_lease_94 = df_ec[df_ec["remaining_lease"] == MOP_REMAINING_LEASE].copy()
    df_lease_89 = df_ec[df_ec["remaining_lease"] == PRIVATISED_REMAINING_LEASE].copy()

    if df_lease_94.empty:
        raise RuntimeError(f"No rows with remaining_lease == {MOP_REMAINING_LEASE}")
    if df_lease_89.empty:
        raise RuntimeError(f"No rows with remaining_lease == {PRIVATISED_REMAINING_LEASE}")

    yield _log_line(f"   Lease 94 (MOP): {len(df_lease_94):,} rows")
    yield _log_line(f"   Lease 89 (Privatised): {len(df_lease_89):,} rows")

    yield _log_line("⚙️  Fitting preprocessing pipeline on all EC data...")
    pipeline = build_sklearn_pipeline()
    pipeline.fit(df_ec)
    df_94_processed = pipeline.transform(df_lease_94)
    df_89_processed = pipeline.transform(df_lease_89)

    yield _log_line(f"🏋️  Training Lease 94 (MOP) model — {df_94_processed.shape[1]} features, RFECV 5-fold CV...")
    rf_94, rfe_94, _, feat_94, res_94 = _train_single(df_94_processed, "Lease 94 (MOP)")
    yield _log_line(f"   ✅ Done — {res_94.n_features_selected} features selected | RMSE: {res_94.rmse:.2f} | R²: {res_94.r2:.4f}")

    yield _log_line(f"🏋️  Training Lease 89 (Privatised) model — {df_89_processed.shape[1]} features, RFECV 5-fold CV...")
    rf_89, rfe_89, _, feat_89, res_89 = _train_single(df_89_processed, "Lease 89 (Privatised)")
    yield _log_line(f"   ✅ Done — {res_89.n_features_selected} features selected | RMSE: {res_89.rmse:.2f} | R²: {res_89.r2:.4f}")

    yield _log_line("💾 Saving model artefacts to disk...")
    joblib.dump(pipeline, MODELS_DIR / "preprocessing_pipeline.joblib")
    joblib.dump(rf_94, MODELS_DIR / "rf_lease_94.joblib")
    joblib.dump(rfe_94, MODELS_DIR / "rfe_lease_94.joblib")
    joblib.dump(rf_89, MODELS_DIR / "rf_lease_89.joblib")
    joblib.dump(rfe_89, MODELS_DIR / "rfe_lease_89.joblib")

    result = {
        "lease_94_mop": {
            "rows": int(res_94.n_rows),
            "features_selected": int(res_94.n_features_selected),
            "selected_features": [str(f) for f in res_94.selected_features],
            "rmse": round(float(res_94.rmse), 2),
            "r2": round(float(res_94.r2), 4),
        },
        "lease_89_privatised": {
            "rows": int(res_89.n_rows),
            "features_selected": int(res_89.n_features_selected),
            "selected_features": [str(f) for f in res_89.selected_features],
            "rmse": round(float(res_89.rmse), 2),
            "r2": round(float(res_89.r2), 4),
        },
    }
    yield json.dumps({"type": "result", "data": result}) + "\n"
