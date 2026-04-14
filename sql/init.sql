-- Initialise the EC prices schema.
-- Runs automatically when the Postgres Docker container starts for the first time.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS properties (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project        TEXT NOT NULL,
    street         TEXT,
    market_segment TEXT NOT NULL CHECK (market_segment IN ('CCR', 'RCR', 'OCR')),
    x              NUMERIC,
    y              NUMERIC,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT properties_project_street_uq UNIQUE (project, street)
);

COMMENT ON TABLE  properties               IS 'One row per EC project.';
COMMENT ON COLUMN properties.market_segment IS 'CCR, RCR, or OCR.';
COMMENT ON COLUMN properties.x             IS 'SVY21 x coordinate.';
COMMENT ON COLUMN properties.y             IS 'SVY21 y coordinate.';

CREATE TABLE IF NOT EXISTS property_transactions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id   UUID    NOT NULL REFERENCES properties (id) ON DELETE CASCADE,
    property_type TEXT    NOT NULL,
    district      TEXT    NOT NULL,
    tenure        TEXT    NOT NULL,
    type_of_sale  SMALLINT NOT NULL CHECK (type_of_sale IN (1, 2, 3)),
    no_of_units   INTEGER NOT NULL CHECK (no_of_units >= 1),
    price         NUMERIC NOT NULL,
    nett_price    NUMERIC,
    area          NUMERIC NOT NULL,
    type_of_area  TEXT    NOT NULL CHECK (type_of_area IN ('Strata', 'Land', 'Unknown')),
    floor_range   TEXT    NOT NULL,
    contract_date TEXT    NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT property_transactions_bk_uq
        UNIQUE (property_id, contract_date, area, price, floor_range, type_of_sale)
);

COMMENT ON TABLE  property_transactions            IS 'One row per URA transaction record.';
COMMENT ON COLUMN property_transactions.type_of_sale IS '1 New Sale, 2 Sub Sale, 3 Resale.';
COMMENT ON COLUMN property_transactions.contract_date IS 'Sale/option date as mmyy text, e.g. 0715.';

CREATE INDEX IF NOT EXISTS property_transactions_property_id_idx
    ON property_transactions (property_id);

CREATE INDEX IF NOT EXISTS property_transactions_contract_date_idx
    ON property_transactions (contract_date);

CREATE INDEX IF NOT EXISTS property_transactions_type_idx
    ON property_transactions (property_type);
