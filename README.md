# dsad_tech_assignment

### Python Version
3.12

### Activate Virtual Env
```bash
source .venv/bin/activate
```

---

## Architecture

```
                        ┌─────────────────────────────────────┐
                        │           Docker Compose             │
                        │                                      │
  User's browser ──────▶│  Streamlit  ──▶  FastAPI  ──▶  Postgres │
                        │  :8501          :8000        :5432   │
                        └─────────────────────────────────────┘
                                             │
                                             ▼
                                        URA Public API
                                    (data ingestion only)
```

| Service | Role |
|---|---|
| **Streamlit** | Frontend — Predict, Train, and Update Database pages |
| **FastAPI** | Backend — exposes `/predict`, `/train`, `/ingest` endpoints |
| **Postgres** | Local database — stores all URA property transactions |
| **URA API** | External data source — fetched once during database update |

**How it flows:**
1. **Update Database** — FastAPI calls the URA API, normalises the data, and upserts it into Postgres.
2. **Train** — FastAPI reads EC transactions from Postgres, engineers features, and trains two Random Forest models (saved to disk).
3. **Predict** — FastAPI loads the saved models and returns a predicted price per sqm for the inputs provided.

---

## Model monitoring (planned)

### Prediction quality

| Metric | What it tells you |
|---|---|
| **RMSE** | Average error size for predicted **price per sqm** (same units as the target). Lower is better. |
| **R²** | How much of the variance in actual prices the model explains (0–1 on held-out data). A sharp drop after retraining or over time suggests the model is no longer a good fit. |

**How to use it here:** After each `/train`, you already get RMSE and R² per lifecycle model (Lease 94 MOP vs Lease 89 Privatised). For ongoing monitoring, recompute RMSE / R² on a **recent hold-out slice** or on **new URA rows** once real prices exist, and compare to the last training run.

### Data drift (KL divergence)

**KL divergence** compares two probability distributions (e.g. training-time vs recent prediction inputs). A larger KL on a feature means that feature’s distribution has shifted — the model may be less reliable until you retrain or refresh data.

**How to use it here:** At train time, save a snapshot of key numeric inputs (e.g. `district`, `contractYear`, `area`, floor range). On each batch of new predictions or new DB rows, estimate the distribution of those same features and compute **KL(reference ‖ current)** per feature. Alert when KL exceeds a small threshold you tune from history.

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

---

## Automating feature engineering, model selection, and monitoring

- **Single pipeline, scheduled runs:** One job (cron/CI) runs ingest → the same preprocessing code → train, so feature engineering never diverges from what production uses.

- **Time-based validation + auto model pick:** Split by `contract_date` (not only random), try a small set of models/hyperparams, and promote the one with best validation RMSE/R² into fixed artefact paths the API loads.

- **Version everything:** Save a small manifest (git SHA, data cutoff date, row counts, chosen params, RMSE/R²) next to the joblib files so every deploy is auditable and comparable to the last run.

- **Log predictions + nightly checks:** Store each prediction with inputs; nightly, compare new URA actuals to predictions (rolling RMSE) and compare input distributions to training (e.g. KL/PSI) — alert if metrics or drift cross thresholds.
