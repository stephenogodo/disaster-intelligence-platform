import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)


# =====================================================
# MAIN FUNCTION
# =====================================================

def compute_disaster_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute industry-style Disaster Risk Score.

    Risk = Hazard × Exposure × Vulnerability

    Parameters
    ----------
    df : DataFrame
        Feature-engineered FEMA disaster dataset

    Returns
    -------
    DataFrame with risk_score + risk_level
    """

    logging.info("===== COMPUTING DISASTER RISK SCORE =====")

    df = df.copy()

    # -------------------------------------------------
    # 1. HAZARD SCORE
    # Frequency of disaster types per state
    # -------------------------------------------------

    logging.info("Calculating Hazard Score...")

    hazard_counts = (
        df.groupby(["state", "incidentType"])
        .size()
        .reset_index(name="event_count")
    )

    hazard_counts["hazard_frequency"] = (
        hazard_counts.groupby("state")["event_count"]
        .transform(lambda x: x / x.sum())
    )

    df = df.merge(
        hazard_counts[["state", "incidentType", "hazard_frequency"]],
        on=["state", "incidentType"],
        how="left",
    )

    df["hazard_score"] = df["hazard_frequency"].rank(pct=True)

    # -------------------------------------------------
    # 2. EXPOSURE SCORE
    # Financial exposure from obligations
    # -------------------------------------------------

    logging.info("Calculating Exposure Score...")

    if "total_obligated_amount" not in df.columns:
        raise ValueError("total_obligated_amount missing")

    df["exposure_score"] = (
        df["total_obligated_amount"].fillna(0).rank(pct=True)
    )

    # -------------------------------------------------
    # 3. VULNERABILITY SCORE
    # Regional sensitivity to disasters
    # -------------------------------------------------

    logging.info("Calculating Vulnerability Score...")

    vulnerability = (
        df.groupby("state")
        .agg(
            avg_cost=("total_obligated_amount", "mean"),
            avg_duration=("incident_duration_days", "mean"),
            disaster_count=("disasterNumber", "count"),
        )
        .reset_index()
    )

    # Normalize vulnerability metrics
    for col in ["avg_cost", "avg_duration", "disaster_count"]:
        vulnerability[col] = vulnerability[col].rank(pct=True)

    vulnerability["vulnerability_score"] = (
        0.4 * vulnerability["avg_cost"]
        + 0.3 * vulnerability["avg_duration"]
        + 0.3 * vulnerability["disaster_count"]
    )

    df = df.merge(
        vulnerability[["state", "vulnerability_score"]],
        on="state",
        how="left",
    )

    # -------------------------------------------------
    # 4. FINAL DISASTER RISK SCORE
    # -------------------------------------------------

    logging.info("Combining Risk Components...")

    df["risk_score"] = (
        0.40 * df["hazard_score"]
        + 0.35 * df["exposure_score"]
        + 0.25 * df["vulnerability_score"]
    )

    # -------------------------------------------------
    # 5. RISK LEVEL CLASSIFICATION
    # -------------------------------------------------

    df["risk_level"] = pd.qcut(
        df["risk_score"],
        q=4,
        labels=["Low", "Moderate", "High", "Extreme"],
    )

    logging.info("===== RISK SCORE COMPLETE =====")

    return df