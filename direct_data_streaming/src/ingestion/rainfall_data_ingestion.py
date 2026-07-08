"""
Rainfall Intensity Ingestion — NOAA GHCN-Daily
TerraNova Resilience Analytics – FEMA Disaster Cost Forecasting Framework

PRODUCTION VERSION
Memory optimizations:
1. Processes one year at a time.
2. Processes one state at a time.
3. Avoids massive state-level merge explosions.
4. Aggressively frees memory.
5. Uses categoricals where appropriate.
6. Maintains only per-disaster maxima.
"""

import gc
import logging
from pathlib import Path

import pandas as pd
import requests

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent
_DS_ROOT = _SRC.parent

RAW_DIR = _DS_ROOT / "data" / "raw"
GHCN_DIR = RAW_DIR / "ghcn"
PROC_DIR = _DS_ROOT / "data" / "processed"

GHCN_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
LOG_DIR = _HERE / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "rainfall_ingest.log"),
    ],
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
GHCN_BASE = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/"
STATIONS_URL = GHCN_BASE + "ghcnd-stations.txt"
BY_YEAR_URL_TEMPLATE = GHCN_BASE + "by_year/{year}.csv.gz"

START_YEAR = 1996
END_YEAR = 2026

DATE_BUFFER_DAYS = 3

# Reduced from 1M to reduce memory spikes
CHUNK_SIZE = 250_000

MISSING_SENTINEL = -9999


# ─────────────────────────────────────────────
# STATION METADATA
# ─────────────────────────────────────────────
def download_stations() -> Path:
    dest = GHCN_DIR / "ghcnd-stations.txt"

    if dest.exists():
        logger.info("  Station metadata already cached.")
        return dest

    logger.info("Downloading station metadata...")

    resp = requests.get(
        STATIONS_URL,
        timeout=60,
    )
    resp.raise_for_status()

    dest.write_bytes(resp.content)

    logger.info(
        f"  Saved ({len(resp.content)/1e6:.1f} MB)"
    )

    return dest


def load_us_stations() -> pd.DataFrame:
    path = download_stations()

    logger.info("Parsing station metadata...")

    colspecs = [
        (0, 11),
        (12, 20),
        (21, 30),
        (31, 37),
        (38, 40),
        (41, 71),
    ]

    names = [
        "ID",
        "LATITUDE",
        "LONGITUDE",
        "ELEVATION",
        "STATE",
        "NAME",
    ]

    stations = pd.read_fwf(
        path,
        colspecs=colspecs,
        names=names,
    )

    before = len(stations)

    stations = stations[
        stations["ID"].str.startswith(
            "US",
            na=False,
        )
    ]

    logger.info(
        f"  Kept {len(stations)}/{before} "
        "US-prefixed stations"
    )

    stations["state"] = (
        stations["STATE"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    stations = stations[
        ["ID", "state"]
    ].dropna()

    stations["state"] = (
        stations["state"]
        .astype("category")
    )

    return stations


# ─────────────────────────────────────────────
# DOWNLOAD GHCN FILES
# ─────────────────────────────────────────────
def download_by_year_files():

    for year in range(
        START_YEAR,
        END_YEAR + 1,
    ):

        dest = GHCN_DIR / f"{year}.csv.gz"

        if dest.exists():
            logger.info(
                f"  {year}: already cached"
            )
            continue

        url = (
            BY_YEAR_URL_TEMPLATE
            .format(year=year)
        )

        logger.info(
            f"  {year}: downloading..."
        )

        try:
            resp = requests.get(
                url,
                timeout=180,
            )
            resp.raise_for_status()

            dest.write_bytes(
                resp.content
            )

            logger.info(
                f"  {year}: saved "
                f"({len(resp.content)/1e6:.1f} MB)"
            )

        except requests.RequestException as e:
            logger.error(
                f"  {year}: "
                f"download failed: {e}"
            )


# ─────────────────────────────────────────────
# FEMA DECLARATIONS
# ─────────────────────────────────────────────
def load_fema_declarations_with_windows():

    logger.info(
        "Loading FEMA declarations..."
    )

    decl = pd.read_parquet(
        RAW_DIR / "declarations.parquet"
    )

    decl.columns = [
        c.strip().lower()
        for c in decl.columns
    ]

    for col in [
        "declarationdate",
        "incidentbegindate",
        "incidentenddate",
    ]:
        decl[col] = (
            pd.to_datetime(
                decl[col],
                errors="coerce",
                utc=True,
            )
            .dt.tz_localize(None)
        )

    decl["state"] = (
        decl["state"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    decl = decl.dropna(
        subset=[
            "disasternumber",
            "state",
            "incidentbegindate",
        ]
    )

    decl["disasternumber"] = (
        decl["disasternumber"]
        .astype(int)
    )

    decl = (
        decl[
            [
                "disasternumber",
                "state",
                "incidentbegindate",
                "incidentenddate",
            ]
        ]
        .drop_duplicates()
    )

    buffer = pd.Timedelta(
        days=DATE_BUFFER_DAYS
    )

    decl["window_start"] = (
        decl["incidentbegindate"]
        - buffer
    )

    decl["window_end"] = (
        decl["incidentenddate"]
        .fillna(
            decl["incidentbegindate"]
        )
        + buffer
    )

    decl["state"] = (
        decl["state"]
        .astype("category")
    )

    logger.info(
        f"  {len(decl)} declarations ready"
    )

    return decl

    # ─────────────────────────────────────────────
# PROCESS ONE YEAR
# ─────────────────────────────────────────────
def process_year_file(
    path: Path,
    year: int,
    us_station_ids: frozenset,
    stations: pd.DataFrame,
    decl: pd.DataFrame,
) -> pd.Series:
    """
    Memory-efficient yearly processing.

    Strategy:
        1. Read one year in chunks.
        2. Keep only US precipitation rows.
        3. Add state information.
        4. Group precipitation by state.
        5. Process one disaster at a time.
        6. Return only per-disaster maxima.
    """

    year_start = pd.Timestamp(
        year,
        1,
        1,
    )

    year_end = pd.Timestamp(
        year,
        12,
        31,
        23,
        59,
        59,
    )

    decl_year = decl[
        (decl["window_start"] <= year_end)
        &
        (decl["window_end"] >= year_start)
    ].copy()

    if decl_year.empty:
        logger.info(
            f"  {year}: no declarations active"
        )
        return pd.Series(
            dtype=float,
            name="rainfall_intensity",
        )

    logger.info(
        f"  {year}: "
        f"{len(decl_year)} active declarations"
    )

    # -------------------------------------------------
    # Read precipitation file in chunks
    # -------------------------------------------------
    col_names = [
        "ID",
        "DATE",
        "ELEMENT",
        "VALUE",
        "MFLAG",
        "QFLAG",
        "SFLAG",
        "OBSTIME",
    ]

    year_frames = []

    for chunk in pd.read_csv(
        path,
        compression="gzip",
        header=None,
        names=col_names,
        usecols=[
            "ID",
            "DATE",
            "ELEMENT",
            "VALUE",
        ],
        chunksize=CHUNK_SIZE,
        low_memory=False,
    ):

        chunk = chunk[
            (chunk["ELEMENT"] == "PRCP")
            &
            (chunk["ID"].isin(us_station_ids))
        ]

        if chunk.empty:
            continue

        chunk = chunk[
            chunk["VALUE"]
            != MISSING_SENTINEL
        ]

        if chunk.empty:
            continue

        chunk["DATE"] = pd.to_datetime(
            chunk["DATE"],
            format="%Y%m%d",
            errors="coerce",
        )

        chunk = chunk.dropna(
            subset=["DATE"]
        )

        if chunk.empty:
            continue

        chunk["precip_mm"] = (
            chunk["VALUE"] / 10.0
        )

        year_frames.append(
            chunk[
                [
                    "ID",
                    "DATE",
                    "precip_mm",
                ]
            ]
        )

    if not year_frames:
        logger.info(
            f"  {year}: no precipitation rows"
        )
        return pd.Series(
            dtype=float,
            name="rainfall_intensity",
        )

    precip = pd.concat(
        year_frames,
        ignore_index=True,
        copy=False,
    )

    del year_frames
    gc.collect()

    logger.info(
        f"  {year}: "
        f"{len(precip):,} US PRCP rows "
        f"after cleaning"
    )

    # -------------------------------------------------
    # Attach states
    # -------------------------------------------------
    precip = precip.merge(
        stations,
        on="ID",
        how="inner",
        copy=False,
    )

    precip["state"] = (
        precip["state"]
        .astype("category")
    )

    precip = precip[
        [
            "state",
            "DATE",
            "precip_mm",
        ]
    ]

    # -------------------------------------------------
    # Build state lookup dictionary
    # -------------------------------------------------
    precip_by_state = {
        state: grp.sort_values("DATE")
        for state, grp in precip.groupby(
            "state",
            observed=True,
        )
    }

    del precip
    gc.collect()

    decl_by_state = {
        state: grp
        for state, grp in decl_year.groupby(
            "state",
            observed=True,
        )
    }

    # -------------------------------------------------
    # Match disasters to rainfall
    # -------------------------------------------------
    results = []

    for state, disasters in decl_by_state.items():

        precip_state = precip_by_state.get(
            state
        )

        if precip_state is None:
            continue

        if precip_state.empty:
            continue

        for _, disaster in disasters.iterrows():

            rainfall = precip_state[
                (
                    precip_state["DATE"]
                    >= disaster["window_start"]
                )
                &
                (
                    precip_state["DATE"]
                    <= disaster["window_end"]
                )
            ]

            if rainfall.empty:
                continue

            max_rainfall = (
                rainfall["precip_mm"].max()
            )

            results.append(
                (
                    disaster[
                        "disasternumber"
                    ],
                    max_rainfall,
                )
            )

        del precip_state

    del precip_by_state
    del decl_by_state
    gc.collect()

    # -------------------------------------------------
    # Build output
    # -------------------------------------------------
    if not results:
        logger.info(
            f"  {year}: no disaster matches"
        )

        return pd.Series(
            dtype=float,
            name="rainfall_intensity",
        )

    matched = pd.DataFrame(
        results,
        columns=[
            "disasternumber",
            "precip_mm",
        ],
    )

    logger.info(
        f"  {year}: "
        f"{len(matched):,} matches across "
        f"{matched['disasternumber'].nunique()} "
        "disasters"
    )

    result = (
        matched.groupby(
            "disasternumber"
        )["precip_mm"]
        .max()
    )

    del matched
    gc.collect()

    return result


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():

    logger.info(
        "===== RAINFALL INTENSITY "
        "INGESTION STARTED ====="
    )

    # -----------------------------------------
    # Download NOAA files
    # -----------------------------------------
    download_by_year_files()

    # -----------------------------------------
    # Station metadata
    # -----------------------------------------
    stations = load_us_stations()

    us_station_ids = frozenset(
        stations["ID"]
    )

    # -----------------------------------------
    # FEMA declarations
    # -----------------------------------------
    decl = (
        load_fema_declarations_with_windows()
    )

    # -----------------------------------------
    # Running disaster maxima
    # -----------------------------------------
    running_max = pd.Series(
        dtype=float,
        name="rainfall_intensity",
    )

    files = sorted(
        GHCN_DIR.glob(
            "[0-9][0-9][0-9][0-9].csv.gz"
        )
    )

    logger.info(
        f"Found {len(files)} yearly files"
    )

    # -----------------------------------------
    # Process each year
    # -----------------------------------------
    for f in files:

        year = int(f.name[:4])

        logger.info("")
        logger.info(
            "=" * 50
        )
        logger.info(
            f"Processing year {year}"
        )
        logger.info(
            "=" * 50
        )

        try:

            year_result = process_year_file(
                f,
                year,
                us_station_ids,
                stations,
                decl,
            )

            if not year_result.empty:

                running_max = (
                    pd.concat(
                        [
                            running_max,
                            year_result,
                        ]
                    )
                    .groupby(level=0)
                    .max()
                )

                running_max.name = (
                    "rainfall_intensity"
                )

            del year_result
            gc.collect()

            logger.info(
                "Running total disasters "
                f"matched: "
                f"{running_max.notna().sum():,}"
            )

        except Exception as e:

            logger.exception(
                f"{year} failed: {e}"
            )

            gc.collect()

            continue

    # -----------------------------------------
    # Final output
    # -----------------------------------------
    if running_max.empty:

        logger.warning(
            "No rainfall matches found."
        )
        return

    rainfall_agg = (
        running_max
        .reset_index()
        .rename(
            columns={
                "index":
                    "disasternumber"
            }
        )
    )

    rainfall_agg = (
        rainfall_agg
        .dropna(
            subset=[
                "rainfall_intensity"
            ]
        )
    )

    rainfall_agg = (
        rainfall_agg
        .sort_values(
            "disasternumber"
        )
        .reset_index(
            drop=True
        )
    )

    out_path = (
        PROC_DIR
        / "rainfall_aggregated.parquet"
    )

    rainfall_agg.to_parquet(
        out_path,
        index=False,
    )

    matched = (
        rainfall_agg[
            "disasternumber"
        ]
        .nunique()
    )

    total = (
        decl[
            "disasternumber"
        ]
        .nunique()
    )

    match_rate = (
        matched / total * 100
    )

    logger.info("")
    logger.info(
        "=" * 50
    )
    logger.info(
        "FINAL SUMMARY"
    )
    logger.info(
        "=" * 50
    )
    logger.info(
        f"Matched disasters: "
        f"{matched:,}/{total:,}"
    )
    logger.info(
        f"Match rate: "
        f"{match_rate:.1f}%"
    )
    logger.info(
        f"Saved to: {out_path}"
    )
    logger.info(
        "===== RAINFALL INTENSITY "
        "INGESTION COMPLETE ====="
    )


if __name__ == "__main__":
    main()