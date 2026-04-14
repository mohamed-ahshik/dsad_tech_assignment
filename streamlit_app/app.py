"""Streamlit frontend for the EC Price Predictor API."""

import json
import os

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://localhost:8000")


def _error_detail(resp: requests.Response) -> str:
    """Extract a readable error message from any API response."""
    try:
        return resp.json().get("detail", resp.text)
    except Exception:
        return resp.text or f"HTTP {resp.status_code}"

st.set_page_config(
    page_title="EC Price Predictor",
    page_icon="🏢",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

st.sidebar.title("🏢 EC Price Predictor")
page = st.sidebar.radio(
    "Navigate",
    ["🔮 Predict Price", "🏋️ Train Model", "🔄 Update Database"],
)

# ---------------------------------------------------------------------------
# Known towns for the dropdown
# ---------------------------------------------------------------------------

TOWNS = sorted([
    "ANG MO KIO", "BEDOK", "BISHAN", "BUKIT BATOK", "BUKIT MERAH",
    "BUKIT PANJANG", "BUKIT TIMAH", "CENTRAL AREA", "CHOA CHU KANG",
    "CLEMENTI", "GEYLANG", "HOUGANG", "JURONG EAST", "JURONG WEST",
    "KALLANG/WHAMPOA", "MARINE PARADE", "PASIR RIS", "PUNGGOL",
    "QUEENSTOWN", "SEMBAWANG", "SENGKANG", "SERANGOON", "TAMPINES",
    "TENGAH", "TOA PAYOH", "WOODLANDS", "YISHUN",
])

# ---------------------------------------------------------------------------
# Page: Predict Price
# ---------------------------------------------------------------------------

if page == "🔮 Predict Price":
    st.title("🔮 Predict EC Price per sqm")
    st.caption(
        "Enter the unit details below. The model automatically derives town, "
        "region, remaining lease and selects the correct prediction model."
    )

    with st.form("predict_form"):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("Unit Details")
            area = st.number_input("Area (sqm)", min_value=20.0, max_value=500.0, value=100.0, step=1.0)
            floor_range = st.text_input("Floor Range", value="06-10", help="e.g. 06-10")
            no_of_units = st.number_input("No. of Units", min_value=1, max_value=10, value=1)
            type_of_area = st.selectbox("Type of Area", ["Strata", "Land"])

        with col2:
            st.subheader("Sale Details")
            contract_date = st.text_input(
                "Contract Date (MMYY)", value="0625",
                help="Month + last 2 digits of year, e.g. 0625 = June 2025"
            )
            type_of_sale = st.selectbox(
                "Type of Sale",
                options=[1, 2, 3],
                format_func=lambda x: {1: "1 – New Sale", 2: "2 – Sub Sale", 3: "3 – Resale"}[x],
                index=2,
            )
            district = st.number_input("District", min_value=1, max_value=28, value=19)
            market_segment = st.selectbox("Market Segment", ["OCR", "RCR", "CCR"])

        with col3:
            st.subheader("Property Details")
            street = st.text_input("Street Name", value="PUNGGOL DRIVE")
            tenure = st.text_input(
                "Tenure",
                value="99 yrs lease commencing from 2020",
                help="e.g. '99 yrs lease commencing from 2020'"
            )
            property_type = st.text_input(
                "Property Type", value="Executive Condominium", disabled=True
            )

        submitted = st.form_submit_button("🔮 Predict", use_container_width=True, type="primary")

    if submitted:
        payload = {
            "area": area,
            "floorRange": floor_range,
            "noOfUnits": no_of_units,
            "contractDate": contract_date,
            "typeOfSale": type_of_sale,
            "propertyType": "Executive Condominium",
            "district": district,
            "typeOfArea": type_of_area,
            "tenure": tenure,
            "street": street,
            "marketSegment": market_segment,
        }
        with st.spinner("Predicting..."):
            try:
                resp = requests.post(f"{API_URL}/predict", json=payload, timeout=30)
                if resp.status_code == 200:
                    result = resp.json()
                    st.success("Prediction complete!")

                    full_price = result["predicted_price_per_sqm"] * area

                    row1 = st.columns(2)
                    row1[0].metric("Predicted Price / sqm", f"SGD ${result['predicted_price_per_sqm']:,.2f}")
                    row1[1].metric("Full Price (× area)", f"SGD ${full_price:,.2f}")

                    row2 = st.columns(3)
                    row2[0].metric("Model Used", result["model_used"])
                    row2[1].metric("Town", result.get("town") or "Unknown")
                    row2[2].metric("Region", result.get("region") or "Unknown")

                    with st.expander("Full response"):
                        st.json(result)
                else:
                    st.error(f"API error {resp.status_code}: {_error_detail(resp)}")
            except requests.exceptions.ConnectionError:
                st.error(f"Cannot connect to API at {API_URL}. Is the backend running?")

# ---------------------------------------------------------------------------
# Page: Train Model
# ---------------------------------------------------------------------------

elif page == "🏋️ Train Model":
    st.title("🏋️ Train Prediction Models")

    use_existing = st.toggle(
        "Use existing data (skip database update)",
        value=True,
        help="When ON, trains directly from whatever EC transactions are already in the database. "
             "Turn OFF to fetch fresh URA data first (go to Update Database page).",
    )

    if use_existing:
        st.info(
            "Training will use EC transactions **already in the database**. "
            "No URA API calls will be made. This takes **1–2 minutes**."
        )
    else:
        st.warning(
            "⚠️ This will first fetch all 4 URA batches from the live API and update the "
            "database before training. Use the **Update Database** page for that instead."
        )

    if st.button("🚀 Start Training", type="primary", use_container_width=True):
        result = None
        with st.status("Training models…", expanded=True) as status:
            log_placeholder = st.empty()
            logs: list[str] = []
            try:
                with requests.get(f"{API_URL}/train/stream", stream=True, timeout=600) as resp:
                    if resp.status_code != 200:
                        status.update(label="Training failed", state="error")
                        st.error(f"API error {resp.status_code}: {resp.text}")
                    else:
                        for raw_line in resp.iter_lines():
                            if not raw_line:
                                continue
                            try:
                                event = json.loads(raw_line)
                            except json.JSONDecodeError:
                                continue
                            if event.get("type") == "log":
                                logs.append(event["message"])
                                log_placeholder.code("\n".join(logs), language=None)
                            elif event.get("type") == "result":
                                result = event["data"]
                status.update(label="✅ Training complete!", state="complete")
            except requests.exceptions.ConnectionError:
                status.update(label="Connection error", state="error")
                st.error(f"Cannot connect to API at {API_URL}. Is the backend running?")

        if result:
            col_mop, col_priv = st.columns(2)

            with col_mop:
                st.subheader("🏠 Lease 94 — MOP (5-yr)")
                mop = result["lease_94_mop"]
                st.metric("Training Rows", mop["rows"])
                st.metric("Features Selected", mop["features_selected"])
                st.metric("RMSE (SGD/sqm)", f"{mop['rmse']:,.2f}")
                st.metric("R²", f"{mop['r2']:.4f}")
                with st.expander("Selected features"):
                    st.write(mop["selected_features"])

            with col_priv:
                st.subheader("🏙️ Lease 89 — Privatised (10-yr)")
                priv = result["lease_89_privatised"]
                st.metric("Training Rows", priv["rows"])
                st.metric("Features Selected", priv["features_selected"])
                st.metric("RMSE (SGD/sqm)", f"{priv['rmse']:,.2f}")
                st.metric("R²", f"{priv['r2']:.4f}")
                with st.expander("Selected features"):
                    st.write(priv["selected_features"])

# ---------------------------------------------------------------------------
# Page: Update Database
# ---------------------------------------------------------------------------

elif page == "🔄 Update Database":
    st.title("🔄 Update Database")
    st.warning(
        "⚠️ This fetches all 4 URA batches (~140k rows) and upserts them into "
        "the database. It takes **several minutes** and makes live API calls to URA."
    )

    confirm = st.checkbox("I understand this will re-fetch and upsert all URA data")

    if st.button("🔄 Update Database", type="primary", use_container_width=True, disabled=not confirm):
        result = None
        with st.status("Fetching URA data and upserting to database…", expanded=True) as status:
            log_placeholder = st.empty()
            logs: list[str] = []
            try:
                with requests.get(f"{API_URL}/ingest/stream", stream=True, timeout=1200) as resp:
                    if resp.status_code != 200:
                        status.update(label="Ingest failed", state="error")
                        st.error(f"API error {resp.status_code}: {resp.text}")
                    else:
                        for raw_line in resp.iter_lines():
                            if not raw_line:
                                continue
                            try:
                                event = json.loads(raw_line)
                            except json.JSONDecodeError:
                                continue
                            if event.get("type") == "log":
                                logs.append(event["message"])
                                log_placeholder.code("\n".join(logs), language=None)
                            elif event.get("type") == "result":
                                result = event["data"]
                status.update(label="✅ Database updated!", state="complete")
            except requests.exceptions.ConnectionError:
                status.update(label="Connection error", state="error")
                st.error(f"Cannot connect to API at {API_URL}. Is the backend running?")

        if result:
            col1, col2, col3 = st.columns(3)
            col1.metric("Batches Processed", result["batches_processed"])
            col2.metric("Properties Upserted", f"{result['properties_upserted']:,}")
            col3.metric("Transactions Upserted", f"{result['transactions_upserted']:,}")
