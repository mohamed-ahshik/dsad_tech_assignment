"""Centralised database client — local Postgres via SQLAlchemy + psycopg2."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import POSTGRES_DSN

logger = logging.getLogger(__name__)

_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a cached SQLAlchemy engine (singleton)."""
    global _engine
    if _engine is None:
        _engine = create_engine(POSTGRES_DSN, pool_pre_ping=True)
        logger.info("Database engine initialised: %s", POSTGRES_DSN)
    return _engine


@contextmanager
def get_connection() -> Generator:
    """Yield an open connection, auto-committing on exit."""
    with get_engine().begin() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Write operations (used by ingest)
# ---------------------------------------------------------------------------

_INSERT_PROPERTY = text("""
    INSERT INTO properties (project, street, market_segment, x, y)
    VALUES (:project, :street, :market_segment, :x, :y)
    ON CONFLICT (project, street)
    DO UPDATE SET
        market_segment = EXCLUDED.market_segment,
        x              = EXCLUDED.x,
        y              = EXCLUDED.y
    RETURNING id
""")

_INSERT_TRANSACTION = text("""
    INSERT INTO property_transactions
        (property_id, property_type, district, tenure, type_of_sale,
         no_of_units, price, nett_price, area, type_of_area, floor_range, contract_date)
    VALUES
        (:property_id, :property_type, :district, :tenure, :type_of_sale,
         :no_of_units, :price, :nett_price, :area, :type_of_area, :floor_range, :contract_date)
    ON CONFLICT (property_id, contract_date, area, price, floor_range, type_of_sale)
    DO NOTHING
""")


def upsert_property(row: dict[str, Any]) -> str:
    """Upsert a single property row and return its UUID."""
    with get_connection() as conn:
        result = conn.execute(_INSERT_PROPERTY, row)
        return str(result.fetchone()[0])


def upsert_transaction(row: dict[str, Any]) -> None:
    """Upsert a single transaction row."""
    with get_connection() as conn:
        conn.execute(_INSERT_TRANSACTION, row)


_UPSERT_PROPERTY_RETURNING = text("""
    INSERT INTO properties (project, street, market_segment, x, y)
    VALUES (:project, :street, :market_segment, :x, :y)
    ON CONFLICT (project, street)
    DO UPDATE SET
        market_segment = EXCLUDED.market_segment,
        x              = EXCLUDED.x,
        y              = EXCLUDED.y
    RETURNING id, project, street
""")

_BULK_INSERT_TRANSACTION = text("""
    INSERT INTO property_transactions
        (property_id, property_type, district, tenure, type_of_sale,
         no_of_units, price, nett_price, area, type_of_area, floor_range, contract_date)
    VALUES
        (:property_id, :property_type, :district, :tenure, :type_of_sale,
         :no_of_units, :price, :nett_price, :area, :type_of_area, :floor_range, :contract_date)
    ON CONFLICT (property_id, contract_date, area, price, floor_range, type_of_sale)
    DO NOTHING
""")


def upsert_properties_bulk(rows: list[dict[str, Any]]) -> dict[tuple[str, str | None], str]:
    """Upsert properties in a single transaction, return mapping of (project, street) → id.

    Executes one INSERT per row inside a shared transaction — much faster than
    opening a new connection per row, while still supporting RETURNING.
    """
    if not rows:
        return {}
    id_map: dict[tuple[str, str | None], str] = {}
    with get_connection() as conn:
        for row in rows:
            result = conn.execute(_UPSERT_PROPERTY_RETURNING, row)
            r = result.fetchone()
            if r:
                id_map[(r.project, r.street)] = str(r.id)
    return id_map


def upsert_transactions_bulk(rows: list[dict[str, Any]]) -> None:
    """Bulk upsert transactions in one round-trip (executemany, no RETURNING needed)."""
    if not rows:
        return
    with get_connection() as conn:
        conn.execute(_BULK_INSERT_TRANSACTION, rows)


# ---------------------------------------------------------------------------
# Read operations (used by preprocessing / training)
# ---------------------------------------------------------------------------

_SELECT_EC_TRANSACTIONS = text("""
    SELECT
        t.id,
        t.property_id,
        t.property_type,
        t.district,
        t.tenure,
        t.type_of_sale,
        t.no_of_units,
        t.price,
        t.nett_price,
        t.area,
        t.type_of_area,
        t.floor_range,
        t.contract_date,
        p.project,
        p.street,
        p.market_segment,
        p.x,
        p.y
    FROM property_transactions t
    JOIN properties p ON p.id = t.property_id
    WHERE t.property_type = 'Executive Condominium'
""")


def fetch_ec_transactions() -> list[dict[str, Any]]:
    """Return all EC transactions joined with their parent property.

    Filters to Executive Condominium in SQL — avoids pulling all 140k rows.
    """
    with get_connection() as conn:
        result = conn.execute(_SELECT_EC_TRANSACTIONS)
        rows = [dict(r._mapping) for r in result]

    if not rows:
        raise RuntimeError(
            "No Executive Condominium transactions found. "
            "Run POST /ingest to populate the database first."
        )

    logger.info("Fetched %d EC transactions from Postgres.", len(rows))
    return rows
