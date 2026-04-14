# dsad_tech_assignment

### Python Version
3.12

### Activate Virtual Env
```bash
source .venv/bin/activate
```

---

## Postgres Data Model

Two tables with a one-to-many relationship: one **project** → many **transactions**.

```
┌──────────────────────────────────────┐
│             properties               │
├──────────────────┬───────────────────┤
│ id               │ UUID  PK          │
│ project          │ TEXT  NOT NULL    │
│ street           │ TEXT              │
│ market_segment   │ TEXT  (CCR/RCR/OCR)│
│ x                │ NUMERIC (SVY21)   │
│ y                │ NUMERIC (SVY21)   │
│ created_at       │ TIMESTAMPTZ       │
├──────────────────┴───────────────────┤
│ UNIQUE (project, street)             │
└──────────────────┬───────────────────┘
                   │ 1
                   │
                   │ many
┌──────────────────▼───────────────────┐
│         property_transactions        │
├──────────────────┬───────────────────┤
│ id               │ UUID  PK          │
│ property_id      │ UUID  FK → properties.id │
│ property_type    │ TEXT              │
│ district         │ TEXT              │
│ tenure           │ TEXT              │
│ type_of_sale     │ SMALLINT (1/2/3)  │
│ no_of_units      │ INTEGER           │
│ price            │ NUMERIC           │
│ nett_price       │ NUMERIC  nullable │
│ area             │ NUMERIC (sqm)     │
│ type_of_area     │ TEXT  (Strata/Land)│
│ floor_range      │ TEXT  e.g. 06-10  │
│ contract_date    │ TEXT  MMYY        │
│ created_at       │ TIMESTAMPTZ       │
├──────────────────┴───────────────────┤
│ UNIQUE (property_id, contract_date,  │
│         area, price, floor_range,    │
│         type_of_sale)                │
└──────────────────────────────────────┘
```

### Indexes
| Index | Column(s) | Purpose |
|---|---|---|
| `property_transactions_property_id_idx` | `property_id` | FK join speed |
| `property_transactions_contract_date_idx` | `contract_date` | date filtering |
| `property_transactions_type_idx` | `property_type` | EC filter on training |

### Key design decisions
- **UUID primary keys** generated via `pgcrypto`'s `gen_random_uuid()`.
- **Unique constraint on `properties(project, street)`** enables idempotent upserts — re-running ingest will update coordinates / market segment rather than duplicate rows.
- **Composite unique constraint on transactions** prevents duplicate records when the same URA batch is ingested more than once (`ON CONFLICT DO NOTHING`).
- `type_of_sale` values follow URA's convention: `1` = New Sale, `2` = Sub Sale, `3` = Resale.
- `contract_date` is stored as raw URA text (`MMYY`, e.g. `"0625"`) and parsed at query / feature-engineering time.
