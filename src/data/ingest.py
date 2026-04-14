"""Fetch URA data, normalise it, and upsert into Postgres."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Generator

from src.config import URA_BATCHES, URA_KEY
from src.data.ura_client import fetch_batch, refresh_token
from src.database.client import upsert_properties_bulk, upsert_transactions_bulk

logger = logging.getLogger(__name__)

_TYPE_OF_AREA_NORMALISE = {
    "strata": "Strata",
    "land": "Land",
}


def _normalise_type_of_area(raw: str) -> str:
    return _TYPE_OF_AREA_NORMALISE.get(raw.strip().lower(), "Unknown")


def _safe_numeric(val: Any) -> float | None:
    """Return a float, or None if the value is missing / a dash."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "-"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_batch(properties: list[dict]) -> tuple[list[dict], dict[tuple, list[dict]]]:
    """Parse a URA batch into property rows and a mapping of (project, street) → txn rows."""
    prop_rows: list[dict] = []
    txn_map: dict[tuple, list[dict]] = {}

    for prop in properties:
        project = (prop.get("project") or "").strip()
        street = (prop.get("street") or "").strip() or None
        market_segment = (prop.get("marketSegment") or "").strip()
        x = _safe_numeric(prop.get("x"))
        y = _safe_numeric(prop.get("y"))

        if not project or market_segment not in ("CCR", "RCR", "OCR"):
            continue

        key = (project, street)
        prop_rows.append({
            "project": project,
            "street": street,
            "market_segment": market_segment,
            "x": x,
            "y": y,
        })

        txns = []
        for txn in prop.get("transaction", []):
            area_val = _safe_numeric(txn.get("area"))
            price_val = _safe_numeric(txn.get("price"))
            type_of_sale_val = _safe_numeric(txn.get("typeOfSale"))
            if area_val is None or price_val is None or type_of_sale_val is None:
                continue
            txns.append({
                "property_type": (txn.get("propertyType") or "").strip(),
                "district": (txn.get("district") or "").strip(),
                "tenure": (txn.get("tenure") or "").strip(),
                "type_of_sale": int(type_of_sale_val),
                "no_of_units": int(_safe_numeric(txn.get("noOfUnits")) or 1),
                "price": price_val,
                "nett_price": _safe_numeric(txn.get("nettPrice")),
                "area": area_val,
                "type_of_area": _normalise_type_of_area(txn.get("typeOfArea") or ""),
                "floor_range": (txn.get("floorRange") or "-").strip(),
                "contract_date": (txn.get("contractDate") or "").strip(),
            })
        txn_map[key] = txns

    return prop_rows, txn_map


def run_ingest() -> dict[str, Any]:
    """Full ingest pipeline: refresh token → fetch batches → bulk upsert to Postgres.

    Returns a summary dict with row counts.
    """
    token = refresh_token(URA_KEY)

    total_properties = 0
    total_transactions = 0

    for batch_num in URA_BATCHES:
        time.sleep(1)
        raw = fetch_batch(batch_num, URA_KEY, token)
        prop_rows, txn_map = _parse_batch(raw)

        id_map = upsert_properties_bulk(prop_rows)
        total_properties += len(id_map)

        txn_rows = []
        for key, txns in txn_map.items():
            prop_id = id_map.get(key)
            if prop_id is None:
                continue
            for txn in txns:
                txn_rows.append({**txn, "property_id": prop_id})

        upsert_transactions_bulk(txn_rows)
        total_transactions += len(txn_rows)
        logger.info("Batch %d done — %d props, %d txns.", batch_num, len(id_map), len(txn_rows))

    summary = {
        "batches_processed": len(URA_BATCHES),
        "properties_upserted": total_properties,
        "transactions_upserted": total_transactions,
    }
    logger.info("Ingest complete: %s", summary)
    return summary


def _log_line(msg: str) -> str:
    return json.dumps({"type": "log", "message": msg}) + "\n"


def run_ingest_iter() -> Generator[str, None, None]:
    """Generator version of run_ingest — yields progress log lines then a
    final ``result`` JSON line.  Consumed by the ``/ingest/stream`` endpoint.
    """
    yield _log_line("🔑 Refreshing URA access token...")
    token = refresh_token(URA_KEY)
    yield _log_line("   ✅ Token refreshed.")

    total_properties = 0
    total_transactions = 0

    for batch_num in URA_BATCHES:
        yield _log_line(f"📦 Fetching URA batch {batch_num} of {len(URA_BATCHES)}...")
        time.sleep(1)
        raw = fetch_batch(batch_num, URA_KEY, token)
        yield _log_line(f"   Batch {batch_num}: {len(raw):,} properties received. Upserting...")

        prop_rows, txn_map = _parse_batch(raw)
        id_map = upsert_properties_bulk(prop_rows)

        txn_rows = []
        for key, txns in txn_map.items():
            prop_id = id_map.get(key)
            if prop_id is None:
                continue
            for txn in txns:
                txn_rows.append({**txn, "property_id": prop_id})

        upsert_transactions_bulk(txn_rows)

        total_properties += len(id_map)
        total_transactions += len(txn_rows)
        yield _log_line(f"   ✅ Batch {batch_num} done — {len(id_map):,} properties, {len(txn_rows):,} transactions upserted.")

    summary = {
        "batches_processed": len(URA_BATCHES),
        "properties_upserted": total_properties,
        "transactions_upserted": total_transactions,
    }
    logger.info("Ingest complete: %s", summary)
    yield _log_line(f"🎉 Ingest complete — {total_properties:,} properties, {total_transactions:,} transactions across {len(URA_BATCHES)} batches.")
    yield json.dumps({"type": "result", "data": summary}) + "\n"
