"""
Step 2: Data Cleaning & Feature Engineering
TerraNova Resilience Analytics – FEMA Disaster Cost Forecasting Framework

Inputs:  direct_data_streaming/data/raw/declarations.parquet
         direct_data_streaming/data/raw/public_assistance.parquet
         direct_data_streaming/data/raw/disaster_summaries.parquet
         direct_data_streaming/data/processed/climate_aggregated.parquet

Outputs: direct_data_streaming/data/processed/features.csv
         direct_data_streaming/data/processed/cost_aggregated.parquet
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Resolve paths relative to this script's location so the script works
# regardless of which directory you run it from.
# Script lives at: direct_data_streaming/src/processing/clean_and_engineer.py
# Data lives at:   direct_data_streaming/data/
_HERE    = Path(__file__).resolve().parent          # .../src/processing
_SRC     = _HERE.parent                             # .../src
_DS_ROOT = _SRC.parent                               # .../direct_data_streaming

#RAW_DIR  = _DS_ROOT / "data" / "raw"
import argparse

from direct_data_streaming.src.common.data_source import (
    get_raw_directory,
)
PROC_DIR = _DS_ROOT / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# 1. LOAD RAW DATA
# ─────────────────────────────────────────────
def load_raw(raw_dir: Path):

    logging.info("Loading raw parquet files…")

    declarations     = pd.read_parquet(raw_dir / "declarations.parquet")
    public_assistance = pd.read_parquet(raw_dir / "public_assistance.parquet")
    disaster_summaries = pd.read_parquet(raw_dir / "disaster_summaries.parquet")

    logging.info(f"  declarations:      {declarations.shape}")
    logging.info(f"  public_assistance: {public_assistance.shape}")
    logging.info(f"  disaster_summaries:{disaster_summaries.shape}")

    return declarations, public_assistance, disaster_summaries


def load_climate() -> pd.DataFrame:
    """
    Loads NOAA-derived climate exposure data — Storm Events (wind_speed,
    flood_severity) merged with GHCN-Daily rainfall intensity
    (rainfall_intensity). Degrades gracefully if either file is missing.
    """
    logging.info("Loading climate exposure data…")

    storm_path = PROC_DIR / "climate_aggregated.parquet"
    rain_path = PROC_DIR / "rainfall_aggregated.parquet"

    storm = pd.read_parquet(storm_path) if storm_path.exists() else pd.DataFrame(columns=["disasternumber", "wind_speed", "flood_severity"])
    rain = pd.read_parquet(rain_path) if rain_path.exists() else pd.DataFrame(columns=["disasternumber", "rainfall_intensity"])

    logging.info(f"  Storm Events climate data: {storm.shape}" if storm_path.exists() else "  No climate_aggregated.parquet found")
    logging.info(f"  GHCN rainfall data: {rain.shape}" if rain_path.exists() else "  No rainfall_aggregated.parquet found")

    storm["disasternumber"] = pd.to_numeric(storm["disasternumber"], errors="coerce").astype("Int64")
    rain["disasternumber"] = pd.to_numeric(rain["disasternumber"], errors="coerce").astype("Int64")

    return storm.merge(rain, on="disasternumber", how="outer")


# ─────────────────────────────────────────────
# 2. CLEAN DECLARATIONS
# ─────────────────────────────────────────────

def clean_declarations(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning declarations…")

    df = df.copy()

    # Normalise column names to snake_case
    df.columns = [c.strip().lower() for c in df.columns]

    # Parse date columns
    for col in ["declarationdate", "incidentbegindate", "incidentenddate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Drop rows missing the disaster key or declaration date
    before = len(df)
    df = df.dropna(subset=["disasternumber", "declarationdate"])
    logging.info(f"  Dropped {before - len(df)} rows missing disasterNumber/declarationDate")

    # Keep only Major Disaster (DR) and Emergency (EM) declarations
    if "declarationtype" in df.columns:
        df = df[df["declarationtype"].isin(["DR", "EM", "FM"])]

    # De-duplicate: one row per (disasterNumber, state)
    df = df.drop_duplicates(subset=["disasternumber", "state"])

    logging.info(f"  Declarations after cleaning: {df.shape}")
    return df


# ─────────────────────────────────────────────
# 3. CLEAN & AGGREGATE PUBLIC ASSISTANCE
# ─────────────────────────────────────────────

def clean_public_assistance(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning public assistance…")

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Resolve amount column name differences across API versions
    amount_col = None
    for candidate in ["obligatedamount", "federalshareobligated", "federalsharesobligation"]:
        if candidate in df.columns:
            amount_col = candidate
            break

    if amount_col is None:
        raise ValueError(
            f"No recognised amount column found. Available: {df.columns.tolist()}"
        )

    df = df.rename(columns={amount_col: "obligated_amount"})
    df["obligated_amount"] = pd.to_numeric(df["obligated_amount"], errors="coerce").fillna(0)

    # Drop cancelled or zero-funded projects
    before = len(df)
    df = df[df["obligated_amount"] > 0]
    logging.info(f"  Dropped {before - len(df)} zero/cancelled projects")

    df["disasternumber"] = pd.to_numeric(df["disasternumber"], errors="coerce")
    df = df.dropna(subset=["disasternumber"])
    df["disasternumber"] = df["disasternumber"].astype(int)

    # ── Aggregate to disaster level ──────────────────────────────────
    agg = (
        df.groupby("disasternumber")
        .agg(
            total_obligated_amount=("obligated_amount", "sum"),
            project_count=("obligated_amount", "count"),
            avg_project_cost=("obligated_amount", "mean"),
            max_project_cost=("obligated_amount", "max"),
        )
        .reset_index()
    )

    # Category breakdown (wide pivot) if projectcategory is present
    if "projectcategory" in df.columns:
        cat_pivot = (
            df.groupby(["disasternumber", "projectcategory"])["obligated_amount"]
            .sum()
            .unstack(fill_value=0)
            .add_prefix("cat_cost_")
        )
        agg = agg.merge(cat_pivot, on="disasternumber", how="left")

    logging.info(f"  Aggregated to {len(agg)} disaster-level rows")
    agg.to_parquet(PROC_DIR / "cost_aggregated.parquet", index=False)

    return agg


# ─────────────────────────────────────────────
# 4. CLEAN DISASTER SUMMARIES (cross-validation)
# ─────────────────────────────────────────────

def clean_summaries(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning disaster summaries…")

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Coerce the federal obligations field if present
    for col in ["totalfederalshareobligations", "totalobligations", "obligatedamount"]:
        if col in df.columns:
            df = df.rename(columns={col: "summary_total_obligations"})
            df["summary_total_obligations"] = pd.to_numeric(
                df["summary_total_obligations"], errors="coerce"
            )
            break

    df["disasternumber"] = pd.to_numeric(df["disasternumber"], errors="coerce")
    df = df.dropna(subset=["disasternumber"])
    df["disasternumber"] = df["disasternumber"].astype(int)

    return df[["disasternumber", "summary_total_obligations"]].drop_duplicates("disasternumber") \
        if "summary_total_obligations" in df.columns \
        else df[["disasternumber"]].drop_duplicates()


# ─────────────────────────────────────────────
# 5. FEATURE ENGINEERING
# ─────────────────────────────────────────────

# Census region mapping (FEMA state codes → region)
REGION_MAP = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "RI": "Northeast", "VT": "Northeast", "NJ": "Northeast", "NY": "Northeast",
    "PA": "Northeast",
    "IL": "Midwest",  "IN": "Midwest",  "MI": "Midwest",  "OH": "Midwest",
    "WI": "Midwest",  "IA": "Midwest",  "KS": "Midwest",  "MN": "Midwest",
    "MO": "Midwest",  "NE": "Midwest",  "ND": "Midwest",  "SD": "Midwest",
    "DE": "South",    "FL": "South",    "GA": "South",    "MD": "South",
    "NC": "South",    "SC": "South",    "VA": "South",    "DC": "South",
    "WV": "South",    "AL": "South",    "KY": "South",    "MS": "South",
    "TN": "South",    "AR": "South",    "LA": "South",    "OK": "South",
    "TX": "South",
    "AZ": "West",     "CO": "West",     "ID": "West",     "MT": "West",
    "NV": "West",     "NM": "West",     "UT": "West",     "WY": "West",
    "AK": "West",     "CA": "West",     "HI": "West",     "OR": "West",
    "WA": "West",
}

# High-risk incident types (label-encoded severity proxy)
# Fixed lookup, known the instant an incident type is identified —
# no dependency on cost data, no leakage.
INCIDENT_SEVERITY = {
    "Hurricane":          5,
    "Flood":              4,
    "Tornado":            4,
    "Severe Storm":       3,
    "Severe Ice Storm":   3,
    "Winter Storm":       3,
    "Earthquake":         5,
    "Typhoon":            5,
    "Fire":               3,
    "Drought":            2,
    "Snowstorm":          2,
    "Landslide":          3,
    "Chemical":           4,
    "Biological":         4,
    "Dam/Levee Break":    5,
    "Coastal Storm":      3,
    "Other":              1,
}

# Normalize to 0–1 using the dict's OWN fixed min/max (not data-driven),
# so this stays a true constant lookup regardless of which rows are
# present in any given run. Matches the 0–1 scale used by the FastAPI
# schema and Streamlit slider.
_SEV_MIN = min(INCIDENT_SEVERITY.values())
_SEV_MAX = max(INCIDENT_SEVERITY.values())
INCIDENT_SEVERITY_NORM = {
    k: (v - _SEV_MIN) / (_SEV_MAX - _SEV_MIN) for k, v in INCIDENT_SEVERITY.items()
}


def engineer_features(declarations: pd.DataFrame,
                       cost_agg: pd.DataFrame,
                       summaries: pd.DataFrame,
                       climate: pd.DataFrame) -> pd.DataFrame:
    logging.info("Engineering features…")

    # ── Temporal features ──────────────────────────────────────────
    decl = declarations.copy()

    decl["declaration_year"]  = decl["declarationdate"].dt.year
    decl["declaration_month"] = decl["declarationdate"].dt.month
    decl["declaration_quarter"] = decl["declarationdate"].dt.quarter

    # Cyclical encoding — raw integers wrongly imply December (12) and
    # January (1) are "far apart" when they're actually adjacent months.
    # sin/cos preserves the true cyclical distance for the model.
    decl["declaration_month_sin"] = np.sin(2 * np.pi * decl["declaration_month"] / 12)
    decl["declaration_month_cos"] = np.cos(2 * np.pi * decl["declaration_month"] / 12)
    decl["declaration_quarter_sin"] = np.sin(2 * np.pi * decl["declaration_quarter"] / 4)
    decl["declaration_quarter_cos"] = np.cos(2 * np.pi * decl["declaration_quarter"] / 4)

    # Incident duration in days
    if "incidentbegindate" in decl.columns and "incidentenddate" in decl.columns:
        decl["incident_duration_days"] = (
            (decl["incidentenddate"] - decl["incidentbegindate"])
            .dt.total_seconds()
            .div(86_400)
            .clip(lower=0)
            .fillna(0)
        )

    # Days from incident start to declaration (response lag)
    if "incidentbegindate" in decl.columns:
        decl["days_to_declaration"] = (
            (decl["declarationdate"] - decl["incidentbegindate"])
            .dt.total_seconds()
            .div(86_400)
            .clip(lower=0)
            .fillna(0)
        )
    else:
        decl["days_to_declaration"] = 0

    # ── Geographic features ────────────────────────────────────────
    if "state" in decl.columns:
        decl["region"] = decl["state"].map(REGION_MAP).fillna("Other")

    # Historical disaster frequency per state (rolling over full dataset)
    # NOTE: this counts disasters across the ENTIRE dataset, including
    # ones that occurred after any given row's declaration date — a
    # lookahead-bias limitation, flagged but not fixed in this pass.
    
        #decl = decl.merge(state_freq, on="state", how="left")

        # Historical disaster frequency per state — CUMULATIVE COUNT, not total.
    # Previously this counted every disaster a state ever had (including
    # ones declared AFTER the current row), which is lookahead bias: it
    # used information that wouldn't exist yet at prediction time. This
    # version sorts by declaration date and counts only PRIOR disasters
    # in the same state, so a 2005 disaster sees a different (smaller)
    # frequency value than a 2024 disaster in the same state — exactly
    # what would be knowable in real time.
    if "state" in decl.columns:
        decl = decl.sort_values(["declarationdate", "disasternumber"]).reset_index(drop=True)
        decl["state_disaster_frequency"] = decl.groupby("state").cumcount()

    # ── Climate exposure (NOAA Storm Events) ────────────────────────
    # Merged onto `decl` HERE — before the incident-type/severity section
    # below — so the severity-score climate hook can detect wind_speed /
    # flood_severity columns and activate the blended formula. Merging
    # later (e.g. after the cost_agg merge) would be too late: the severity
    # section would already have run and logged "No climate data yet".
    decl["disasternumber"] = pd.to_numeric(decl["disasternumber"], errors="coerce").astype("Int64")
    climate["disasternumber"] = pd.to_numeric(climate["disasternumber"], errors="coerce").astype("Int64")

    #decl = decl.merge(climate, on="disasternumber", how="left")
    #if "wind_speed" in decl.columns:
    #    decl["wind_speed"] = decl["wind_speed"].fillna(0)
    #if "flood_severity" in decl.columns:
    #    decl["flood_severity"] = decl["flood_severity"].fillna(0) 

    decl = decl.merge(climate, on="disasternumber", how="left")
    for col in ["wind_speed", "flood_severity", "rainfall_intensity"]:
        if col in decl.columns:
            decl[col] = decl[col].fillna(0)

    matched = (decl["wind_speed"] > 0) | (decl["flood_severity"] > 0) if "wind_speed" in decl.columns else pd.Series(False, index=decl.index)
    logging.info(f"  Climate-matched disasters: {matched.sum()}/{len(decl)} ({matched.mean()*100:.1f}%)")

    # ── Incident type features ─────────────────────────────────────
    if "incidenttype" in decl.columns:
        type_severity = decl["incidenttype"].map(INCIDENT_SEVERITY_NORM).fillna(0.0)

        # Climate blending — activates automatically now that wind_speed /
        # flood_severity are merged in above. severity_score becomes
        # 60% incident-type + 40% climate intensity wherever a climate
        # match exists; falls back to type-only for the ~28% with no match
        # (their climate columns are 0 after fillna, contributing 0 to
        # the climate component — not ideal, but a documented limitation).
        climate_cols = ["rainfall_intensity", "wind_speed", "flood_severity"]
        available_climate = [c for c in climate_cols if c in decl.columns]

        if available_climate:
            logging.info(f"  Climate data found {available_climate} — blending into severity score")
            climate_norm = decl[available_climate].apply(
                lambda col: (col - col.min()) / (col.max() - col.min() + 1e-9)
            )
            decl["incident_severity_score"] = 0.6 * type_severity + 0.4 * climate_norm.mean(axis=1)
        else:
            logging.info("  No climate data yet — severity score = incident-type lookup only")
            decl["incident_severity_score"] = type_severity

        # One-hot encode top incident types
        top_types = decl["incidenttype"].value_counts().head(10).index.tolist()
        for t in top_types:
            safe = t.replace(" ", "_").replace("/", "_").lower()
            decl[f"is_{safe}"] = (decl["incidenttype"] == t).astype(int)
         
    # ── Interaction features ───────────────────────────────────────
    # Deterministic combinations of existing inputs — no new information,
    # but lets tree models capture non-additive effects (e.g. severity
    # matters more when duration is also long) without needing to
    # rediscover the interaction through many separate splits.
    if "incident_severity_score" in decl.columns:
        decl["severity_x_duration"] = decl["incident_severity_score"] * decl["incident_duration_days"]
        decl["severity_x_frequency"] = decl["incident_severity_score"] * decl["state_disaster_frequency"]
        decl["duration_x_days_to_declaration"] = decl["incident_duration_days"] * decl["days_to_declaration"]


    # ── Merge cost aggregation ─────────────────────────────────────
    cost_agg["disasternumber"] = pd.to_numeric(
        cost_agg["disasternumber"], errors="coerce"
    ).astype("Int64")

    features = decl.merge(cost_agg, on="disasternumber", how="left")

    # ── Cross-validate with summaries ─────────────────────────────
    if "summary_total_obligations" in summaries.columns:
        summaries["disasternumber"] = pd.to_numeric(
            summaries["disasternumber"], errors="coerce"
        ).astype("Int64")
        features = features.merge(summaries, on="disasternumber", how="left")

        # Validation flag: large discrepancy between sources
        features["obligation_discrepancy"] = (
            (features["total_obligated_amount"] - features["summary_total_obligations"])
            .abs()
            .div(features["summary_total_obligations"].replace(0, np.nan))
        )
        features["high_discrepancy_flag"] = (
            features["obligation_discrepancy"] > 0.20
        ).astype(int)

    # ── Target variable: log-transform for skewed costs ───────────
    if "total_obligated_amount" in features.columns:
        features["log_total_obligated_amount"] = np.log1p(
            features["total_obligated_amount"].fillna(0)
        )

    # ── Drop rows with no cost data (no PA match) ─────────────────
    before = len(features)
    features = features.dropna(subset=["total_obligated_amount"])
    logging.info(f"  Dropped {before - len(features)} rows with no cost match")

    # ── Encode region ──────────────────────────────────────────────
    if "region" in features.columns:
        features["region_encoded"] = pd.Categorical(features["region"]).codes

    logging.info(f"  Final feature set: {features.shape}")
    return features


# ─────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────

def main(source: str = "batch"):

    raw_dir = get_raw_directory(source)

    logging.info(f"Using '{source}' data source")
    logging.info(f"Raw data directory: {raw_dir}")

    declarations, public_assistance, disaster_summaries = load_raw(raw_dir)

    climate = load_climate()

    declarations_clean = clean_declarations(declarations)
    cost_aggregated = clean_public_assistance(public_assistance)
    summaries_clean = clean_summaries(disaster_summaries)

    features = engineer_features(
        declarations_clean,
        cost_aggregated,
        summaries_clean,
        climate,
    )

    out_path = PROC_DIR / "features.csv"
    tmp_path = PROC_DIR / "features.csv.tmp"

    try:
        features.to_csv(tmp_path, index=False)
        tmp_path.replace(out_path)

    except PermissionError as e:
        logging.error(
            f"Could not write {out_path}. Details: {e}"
        )
        raise

    logging.info(f"Saved feature set to {out_path}")
    logging.info(f"Columns: {features.columns.tolist()}")
    logging.info("===== STEP 2 COMPLETE =====")


'''if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Feature Engineering Pipeline"
    )

    parser.add_argument(
        "--source",
        default="batch",
        choices=["batch", "kafka"],
        help="Raw data source",
    )

    args = parser.parse_args()

    main(source=args.source)'''

if __name__ == "__main__":
    main()