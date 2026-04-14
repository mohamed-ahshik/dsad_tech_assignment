"""Thin client for the URA private residential transaction API."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from src.config import (
    URA_BASE_URL,
    URA_DATA_URL,
    URA_KEY,
    URA_SERVICE,
    URA_TOKEN_URL,
)

logger = logging.getLogger(__name__)

_HEADERS_BASE = {
    "Accept": "application/json",
    "User-Agent": "curl/8.7.1",
}


def refresh_token(access_key: str | None = None) -> str:
    """Call URA ``insertNewToken`` and return the fresh token string."""
    key = access_key or URA_KEY
    if not key:
        raise RuntimeError("URA_KEY is not set.")

    headers = {**_HEADERS_BASE, "AccessKey": key}
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(URA_TOKEN_URL, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    if data.get("Status") != "Success":
        raise RuntimeError(f"URA token refresh failed: {data}")

    token: str = data["Result"]
    logger.info("URA token refreshed successfully.")
    return token


def fetch_batch(batch: int, access_key: str | None = None, token: str | None = None) -> list[dict[str, Any]]:
    """Fetch a single batch from the URA PMI Resi Transaction endpoint.

    Returns the ``Result`` list (each item is a property with nested
    ``transaction`` array).
    """
    key = access_key or URA_KEY
    tok = token
    if not key or not tok:
        raise RuntimeError("URA_KEY and URA_TOKEN must be provided.")

    headers = {**_HEADERS_BASE, "AccessKey": key, "Token": tok}
    url = f"{URA_DATA_URL}?service={URA_SERVICE}&batch={batch}"

    with httpx.Client(timeout=120.0) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()

    # URA responses may contain non-UTF-8 bytes (e.g. accented characters in
    # street names encoded as Latin-1). Decode raw bytes with latin-1 which
    # accepts any byte value, then parse JSON from the resulting string.
    data = json.loads(resp.content.decode("latin-1"))
    if data.get("Status") != "Success":
        raise RuntimeError(f"URA batch {batch} failed: {data}")

    result: list[dict[str, Any]] = data.get("Result", [])
    logger.info("Batch %d: %d properties fetched.", batch, len(result))
    return result
