"""FastAPI application exposing the EC price-prediction model."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")

app = FastAPI(
    title="EC Price Predictor",
    description="RESTful API for ingesting URA data, training EC price models, and predicting price per sqm.",
    version="0.1.0",
)


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class PredictRequest(BaseModel):
    """Raw URA-style fields. The preprocessing pipeline derives all other features."""

    area: float = Field(..., description="Unit area in sqm", gt=0)
    floorRange: str = Field(..., description="Floor range, e.g. '06-10'")
    noOfUnits: int = Field(default=1, ge=1)
    contractDate: str = Field(..., description="Sale/option date as MMYY, e.g. '0625'")
    typeOfSale: int = Field(..., description="1 = New Sale, 2 = Sub Sale, 3 = Resale")
    propertyType: str = Field(default="Executive Condominium")
    district: int = Field(..., ge=1, le=28)
    typeOfArea: str = Field(default="Strata", description="Strata or Land")
    tenure: str = Field(..., description="e.g. '99 yrs lease commencing from 2020'")
    street: str = Field(..., description="Street name, e.g. 'PUNGGOL DRIVE'")
    marketSegment: str = Field(..., description="CCR, RCR, or OCR")


class PredictResponse(BaseModel):
    model_used: str
    remaining_lease: float
    town: str | None
    region: str | None
    predicted_price_per_sqm: float


class TrainResponse(BaseModel):
    lease_94_mop: dict[str, Any]
    lease_89_privatised: dict[str, Any]


class IngestResponse(BaseModel):
    batches_processed: int
    properties_upserted: int
    transactions_upserted: int


class TransactionRecord(BaseModel):
    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/transactions")
def get_transactions():
    """Return all EC transactions from Supabase (used for training data preview)."""
    try:
        from src.database.client import fetch_ec_transactions

        rows = fetch_ec_transactions()
        return {"count": len(rows), "data": rows}
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/ingest", response_model=IngestResponse)
def ingest():
    """Fetch data from URA, normalise, and upsert into Supabase."""
    try:
        from src.data.ingest import run_ingest

        summary = run_ingest()
        return summary
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/train", response_model=TrainResponse)
def train():
    """Read data from Supabase, train both models, and save artefacts."""
    try:
        from src.models.trainer import train_models

        result = train_models()
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/train/stream")
def train_stream():
    """Stream training progress as newline-delimited JSON."""
    from src.models.trainer import train_models_iter

    return StreamingResponse(train_models_iter(), media_type="application/x-ndjson")


@app.get("/ingest/stream")
def ingest_stream():
    """Stream ingest progress as newline-delimited JSON."""
    from src.data.ingest import run_ingest_iter

    return StreamingResponse(run_ingest_iter(), media_type="application/x-ndjson")


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    """Predict price_per_sqm for a single EC unit."""
    try:
        from src.models.predictor import predict as do_predict

        result = do_predict(req.model_dump())
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
