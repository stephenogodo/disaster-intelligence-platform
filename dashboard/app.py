# dashboard/app.py
import logging
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "dashboard.log"),
    ]
)
logger = logging.getLogger(__name__)

#API_URL = "http://127.0.0.1:8000"
import os
API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "B").strip().upper()

st.set_page_config(page_title="FEMA Cost Forecaster", layout="wide", page_icon="🌪️")

st.title("🌪️ Disaster Recovery Cost Forecasting Framework")
st.markdown("**TerraNova Resilience Analytics Ltd** &nbsp;|&nbsp; Climate Risk & Public Sector Analytics")
st.divider()

# ── Reference table: fixed incident-type severity baseline ───────────────────
# Matches INCIDENT_SEVERITY_NORM in clean_and_engineer.py — used only to show
# the user a sensible starting point; the actual model-side severity score
# during training also blends in NOAA climate intensity (wind speed, flood
# event counts) for ~72% of historical disasters. Since this API takes a
# single severity_score input rather than raw climate fields, the dashboard
# treats severity as a slider the user sets directly, anchored to this baseline.
TYPE_SEVERITY_BASELINE = {
    "Hurricane": 1.00, "Typhoon": 1.00,
    "Flood": 0.75, "Tornado": 0.75, "Biological": 0.75,
    "Severe Storm": 0.50, "Fire": 0.50, "Severe Ice Storm": 0.50,
    "Drought": 0.25, "Snowstorm": 0.25,
    "Other": 0.00,
}

incident_options = {
    "Hurricane": "is_hurricane",
    "Flood": "is_flood",
    "Fire": "is_fire",
    "Severe Storm": "is_severe_storm",
    "Tornado": "is_tornado",
    "Snowstorm": "is_snowstorm",
    "Severe Ice Storm": "is_severe_ice_storm",
    "Typhoon": "is_typhoon",
    "Drought": "is_drought",
    "Biological": "is_biological",
}

# ── Sidebar: Scenario Inputs ──────────────────────────────────────────────────
st.sidebar.header("📋 Scenario Parameters")

model_choice = st.sidebar.selectbox(
    "Forecast Model",
    options=["B", "A"],
    format_func=lambda value: (
        "Model B — Early Forecast"
        if value == "B"
        else "Model A — Standard Forecast"
    ),
    index=0 if MODEL_VERSION == "B" else 1,
)

st.sidebar.caption(
    "Model B forecasts without incident duration. "
    "Model A uses incident duration for standard forecasting."
)

selected_incidents = st.sidebar.multiselect(
    "Incident Type(s)", list(incident_options.keys()), default=["Hurricane", "Flood"]
)

region = st.sidebar.slider("FEMA Region (encoded)", 0, 9, 4)
if model_choice == "A":
    duration = st.sidebar.slider(
        "Incident Duration (days)",
        1,
        180,
        14,
    )
else:
    duration = None
    st.sidebar.info(
        "Model B: incident duration is not required for an early forecast."
    )

days_to_decl = st.sidebar.slider("Days to Declaration", 0, 60, 7)
frequency = st.sidebar.slider("State Disaster Frequency (prior events)", 0, 100, 12)

# Suggested severity baseline based on selection — shown as guidance only,
# since the slider below is what's actually sent to the model.
if selected_incidents:
    suggested = sum(TYPE_SEVERITY_BASELINE.get(t, 0.0) for t in selected_incidents) / len(selected_incidents)
else:
    suggested = 0.0

st.sidebar.caption(
    f"💡 Suggested baseline for selected type(s): **{suggested:.2f}** "
    f"(historical type severity only — adjust upward if you have intel on "
    f"actual storm intensity, e.g. high wind speeds or major flooding)"
)
severity = st.sidebar.slider("Incident Severity Score", 0.0, 1.0, float(suggested), step=0.05)


decl_date = st.sidebar.date_input("Declaration Date", datetime.now().date(),)

st.sidebar.divider()
predict_clicked = st.sidebar.button("🔮 Forecast Recovery Cost", use_container_width=True)

# ── Build payload ──────────────────────────────────────────────────────────────
def build_payload():
    payload = {
        "region_encoded": region,
        "days_to_declaration": days_to_decl,
        "state_disaster_frequency": frequency,
        "incident_severity_score": severity,
        "declaration_year": decl_date.year,
        "declaration_month": decl_date.month,
        "declaration_quarter": (decl_date.month - 1) // 3 + 1,
    }

    # Model A requires incident duration.
    # Model B intentionally does not receive it.
    if model_choice == "A":
        payload["incident_duration_days"] = duration

    for label, field in incident_options.items():
        payload[field] = 1 if label in selected_incidents else 0

    return payload


# ── Main panel ────────────────────────────────────────────────────────────────
col1, col2 = st.columns([2, 1])

if predict_clicked:
    payload = build_payload()
    logger.info(f"Sending prediction request: {payload}")

    try:
        response = requests.post(
            f"{API_URL}/predict-cost",
            json=payload,
            headers={"X-Model-Version": model_choice},
            timeout=10,
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Prediction received: {result}")

        cost = result["predicted_recovery_cost_usd"]

        with col1:
            st.success(f"### 💰 Predicted Recovery Cost: **${cost:,.0f}**")
            st.caption(
                f"Model: {result['model_type']} | "
                f"Forecast: {result.get('forecast_type', 'Unknown')} | "
                f"Generated: {result['timestamp']}"
            )

            st.subheader("📊 Budget Gap Analysis")
            st.markdown("How a funding gap would look under different budget allocation scenarios:")

            allocations = [0.5, 0.7, 0.9, 1.0, 1.1, 1.3]
            labels = [f"{int(a*100)}%" for a in allocations]
            budgets = [cost * a for a in allocations]
            gaps = [b - cost for b in budgets]

            gap_df = pd.DataFrame({"Budget Allocation": labels, "Gap (USD)": gaps})
            fig = px.bar(
                gap_df, x="Budget Allocation", y="Gap (USD)",
                title="Funding Surplus / Shortfall vs. Forecast",
                color="Gap (USD)", color_continuous_scale="RdYlGn",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🧾 Scenario Summary")
            st.json(payload)

    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to API")
        st.error("⚠️ Could not connect to the API. Is it running at " + API_URL + "?")
    except requests.exceptions.HTTPError as e:
        logger.error(f"API returned an error: {e}")
        st.error(f"⚠️ API error: {response.text}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        st.error(f"⚠️ Unexpected error: {e}")

else:
    with col1:
        st.info("👈 Set scenario parameters in the sidebar and click **Forecast Recovery Cost** to begin.")
    with col2:
        st.subheader("ℹ️ About")
        st.markdown(
            "This dashboard forecasts disaster recovery costs using a model trained "
            "on historical FEMA disaster declarations, FEMA Public Assistance funding "
            "data, and NOAA Storm Events climate intensity data (wind speed, flood "
            "event frequency), blended into a declaration-time severity score."
        )

st.divider()
st.caption("FEMA Disaster Recovery Cost Forecasting Framework | TerraNova Resilience Analytics Ltd")