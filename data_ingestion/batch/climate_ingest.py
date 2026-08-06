"""
Climate Exposure Ingestion — NOAA Storm Events Database
TerraNova Resilience Analytics – FEMA Disaster Cost Forecasting Framework

Downloads NOAA Storm Events "details" files (1996–present), matches them to
FEMA disasters by county FIPS + incident date-range overlap, and aggregates
to one row per disasterNumber.

LIMITATIONS (documented, not bugs):
  - No true rainfall/precipitation measure exists in Storm Events. This script
    produces wind_speed and flood_severity only. True rainfall_intensity would
    require a separate NOAA precipitation dataset (e.g. GHCN-Daily) — deferred.
  - Comprehensive multi-hazard coverage starts in 1996. Disasters before that
    will have no climate match (expected, not an error).
  - The join is fuzzy (FIPS + date overlap), not an exact key — check the
    logged match rate before trusting this data downstream.

Inputs:
    data/raw/declarations.parquet

Outputs:
    data/processed/climate_aggregated.parquet
    data/raw/noaa/StormEvents_details-*.csv.gz (cached)
"""

import re
import logging
import requests
import pandas as pd
import numpy as np
from pathlib import Path

from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
from storage.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
)

RAW_DIR = RAW_DATA_DIR
PROC_DIR = PROCESSED_DATA_DIR

NOAA_DIR = RAW_DIR / "noaa"
NOAA_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
LOG_DIR = CURRENT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "climate_ingest.log")],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
INDEX_URL = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
START_YEAR = 1996   # comprehensive multi-hazard coverage begins here
END_YEAR   = 2026
DATE_BUFFER_DAYS = 3  # tolerance window around incident begin/end dates

WIND_MAGNITUDE_TYPES = {"EG", "ES", "MG", "MS"}  # estimated/measured gust/sustained
FLOOD_EVENT_TYPES = {"Flood", "Flash Flood", "Coastal Flood", "Lakeshore Flood"}


# ─────────────────────────────────────────────
# 1. DISCOVER & DOWNLOAD NOAA FILES
# ─────────────────────────────────────────────

def discover_noaa_files() -> dict:
    """Scrape NOAA's index page to find the exact filenames per year
    (the version-date suffix changes whenever NOAA reprocesses a year,
    so it can't be hardcoded)."""
    logger.info(f"Discovering NOAA Storm Events files at {INDEX_URL}")
    resp = requests.get(INDEX_URL, timeout=30)
    resp.raise_for_status()

    pattern = re.compile(r'StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d+)\.csv\.gz')
    matches = pattern.findall(resp.text)

    year_to_version = {}
    for year_str, version in matches:
        year = int(year_str)
        if START_YEAR <= year <= END_YEAR:
            if year not in year_to_version or version > year_to_version[year]:
                year_to_version[year] = version

    urls = {
        year: f"{INDEX_URL}StormEvents_details-ftp_v1.0_d{year}_c{version}.csv.gz"
        for year, version in year_to_version.items()
    }
    logger.info(f"Found {len(urls)} years available ({min(urls)}–{max(urls)})" if urls else "No files found")
    return urls


def download_noaa_files(urls: dict):
    for year, url in sorted(urls.items()):
        dest = NOAA_DIR / f"{year}.csv.gz"
        if dest.exists():
            logger.info(f"  {year}: already cached, skipping")
            continue
        logger.info(f"  {year}: downloading from {url}")
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            logger.info(f"  {year}: saved ({len(resp.content) / 1e6:.1f} MB)")
        except requests.RequestException as e:
            logger.error(f"  {year}: download failed — {e}")


# ─────────────────────────────────────────────
# 2. LOAD & CLEAN NOAA EVENTS
# ─────────────────────────────────────────────

def load_noaa_events() -> pd.DataFrame:
    logger.info("Loading cached NOAA Storm Events files…")
    files = sorted(NOAA_DIR.glob("*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"No NOAA files found in {NOAA_DIR} — run download step first")

    target_cols = {"STATE_FIPS", "CZ_TYPE", "CZ_FIPS", "BEGIN_DATE_TIME",
                    "END_DATE_TIME", "EVENT_TYPE", "MAGNITUDE", "MAGNITUDE_TYPE"}

    def _keep_col(c):
        return c.strip().upper() in target_cols

    frames = []
    for f in files:
        try:
            # Select only the needed columns AT READ TIME — loading all ~50+
            # raw NOAA columns across 1.79M rows before narrowing down is
            # what caused the MemoryError last run.
            df = pd.read_csv(f, compression="gzip", low_memory=False, usecols=_keep_col)
            df.columns = [c.strip().upper() for c in df.columns]
            for col in target_cols:
                if col not in df.columns:
                    df[col] = np.nan  # some older years may lack a column
            frames.append(df[list(target_cols)])
        except Exception as e:
            logger.warning(f"  Skipping {f.name}: {e}")

    events = pd.concat(frames, ignore_index=True)
    del frames
    logger.info(f"  Loaded {len(events)} raw event records (relevant columns only) across {len(files)} files")

    before = len(events)
    events = events[events["CZ_TYPE"] == "C"]
    logger.info(f"  Kept {len(events)}/{before} county-level (CZ_TYPE='C') records")

    events["STATE_FIPS"] = pd.to_numeric(events["STATE_FIPS"], errors="coerce").astype("Int64").astype(str).str.zfill(2)
    events["CZ_FIPS"]    = pd.to_numeric(events["CZ_FIPS"], errors="coerce").astype("Int64").astype(str).str.zfill(3)
    events["fips_key"]   = events["STATE_FIPS"] + events["CZ_FIPS"]

    # NOAA's date format is "DD-MON-YY HH:MM:SS" — specifying it directly
    # avoids the slow per-row dateutil fallback warning from last run.
    events["BEGIN_DATE_TIME"] = pd.to_datetime(events["BEGIN_DATE_TIME"], format="%d-%b-%y %H:%M:%S", errors="coerce")
    events["END_DATE_TIME"]   = pd.to_datetime(events["END_DATE_TIME"], format="%d-%b-%y %H:%M:%S", errors="coerce")

    events = events.dropna(subset=["fips_key", "BEGIN_DATE_TIME"])

    cols = ["fips_key", "BEGIN_DATE_TIME", "END_DATE_TIME", "EVENT_TYPE", "MAGNITUDE", "MAGNITUDE_TYPE"]
    logger.info(f"  Final cleaned NOAA event set: {len(events)} records")
    return events[cols]


# ─────────────────────────────────────────────
# 3. LOAD MINIMAL FEMA DECLARATIONS (for matching)
# ─────────────────────────────────────────────

def load_fema_declarations_min() -> pd.DataFrame:
    logger.info("Loading FEMA declarations for matching…")
    decl = pd.read_parquet(RAW_DIR / "declarations.parquet")
    decl.columns = [c.strip().lower() for c in decl.columns]

    for col in ["declarationdate", "incidentbegindate", "incidentenddate"]:
        decl[col] = pd.to_datetime(decl[col], errors="coerce", utc=True).dt.tz_localize(None)

    decl["fipsstatecode"]  = pd.to_numeric(decl["fipsstatecode"], errors="coerce").astype("Int64").astype(str).str.zfill(2)
    decl["fipscountycode"] = pd.to_numeric(decl["fipscountycode"], errors="coerce").astype("Int64").astype(str).str.zfill(3)
    decl["fips_key"] = decl["fipsstatecode"] + decl["fipscountycode"]

    decl = decl.dropna(subset=["disasternumber", "fips_key", "incidentbegindate"])
    decl["disasternumber"] = decl["disasternumber"].astype(int)

    logger.info(f"  {len(decl)} declaration records ready for matching")
    return decl[["disasternumber", "fips_key", "incidentbegindate", "incidentenddate"]].drop_duplicates()


# ─────────────────────────────────────────────
# 4. MATCH NOAA EVENTS TO FEMA DISASTERS
# ─────────────────────────────────────────────

def match_events_to_disasters(decl: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    logger.info("Matching NOAA events to FEMA disasters (FIPS + date overlap)…")

    merged = decl.merge(events, on="fips_key", how="inner")
    logger.info(f"  {len(merged)} candidate matches after FIPS join")

    buffer = pd.Timedelta(days=DATE_BUFFER_DAYS)
    window_start = merged["incidentbegindate"] - buffer
    window_end = merged["incidentenddate"].fillna(merged["incidentbegindate"]) + buffer

    in_window = (merged["BEGIN_DATE_TIME"] >= window_start) & (merged["BEGIN_DATE_TIME"] <= window_end)
    matched = merged[in_window]

    logger.info(f"  {len(matched)} matches survive the ±{DATE_BUFFER_DAYS}-day date window")

    matched_disasters = matched["disasternumber"].nunique()
    total_disasters = decl["disasternumber"].nunique()
    match_rate = matched_disasters / total_disasters * 100
    logger.info(
        f"  MATCH RATE: {matched_disasters}/{total_disasters} disasters ({match_rate:.1f}%) "
        f"got at least one climate event match"
    )
    if match_rate < 30:
        logger.warning(
            "  Match rate is low — check FIPS formatting, date buffer, or year coverage "
            "before trusting this climate data downstream."
        )

    return matched


# ─────────────────────────────────────────────
# 5. AGGREGATE TO DISASTER LEVEL
# ─────────────────────────────────────────────

def aggregate_climate_by_disaster(matched: pd.DataFrame) -> pd.DataFrame:
    logger.info("Aggregating matched events to disaster level…")

    is_wind = matched["MAGNITUDE_TYPE"].isin(WIND_MAGNITUDE_TYPES)
    is_flood = matched["EVENT_TYPE"].isin(FLOOD_EVENT_TYPES)

    wind_speed = (
        matched[is_wind]
        .groupby("disasternumber")["MAGNITUDE"]
        .max()
        .rename("wind_speed")
    )

    flood_severity = (
        matched[is_flood]
        .groupby("disasternumber")
        .size()
        .rename("flood_severity")
    )

    storm_event_count = (
        matched.groupby("disasternumber")
        .size()
        .rename("storm_event_count")  # diagnostic — not fed to the model
    )

    agg = pd.concat([wind_speed, flood_severity, storm_event_count], axis=1).reset_index()
    logger.info(f"  Aggregated climate data for {len(agg)} disasters")
    logger.info(f"  wind_speed populated for {agg['wind_speed'].notna().sum()} disasters")
    logger.info(f"  flood_severity populated for {agg['flood_severity'].notna().sum()} disasters")

    return agg


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logger.info("===== CLIMATE EXPOSURE INGESTION STARTED =====")

    urls = discover_noaa_files()
    download_noaa_files(urls)

    events = load_noaa_events()
    decl = load_fema_declarations_min()
    matched = match_events_to_disasters(decl, events)
    climate_agg = aggregate_climate_by_disaster(matched)

    out_path = PROC_DIR / "climate_aggregated.parquet"
    climate_agg.to_parquet(out_path, index=False)
    logger.info(f"Saved → {out_path}")
    logger.info("===== CLIMATE EXPOSURE INGESTION COMPLETE =====")


if __name__ == "__main__":
    main()