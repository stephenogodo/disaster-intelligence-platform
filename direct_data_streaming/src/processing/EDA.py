"""
Step 3: Exploratory Data Analysis (EDA)
TerraNova Resilience Analytics – FEMA Disaster Cost Forecasting Framework

Input:  direct_data_streaming/data/processed/features.csv

Outputs (all saved to direct_data_streaming/reports/eda/):
  - cost_distribution.png        Cost distribution by incident type
  - geographic_funding.png       Total obligated amount by state
  - duration_vs_cost.png         Incident duration vs recovery cost
  - cost_over_time.png           Annual trend of total obligations
  - outlier_analysis.png         Box-plot outlier detection
  - correlation_heatmap.png      Feature correlation matrix
  - eda_summary.csv              Summary statistics table
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Resolve paths relative to this script's location so the script works
# regardless of which directory you run it from.
_HERE    = Path(__file__).resolve().parent          # .../src/processing
_SRC     = _HERE.parent                             # .../src
_DS_ROOT = _SRC.parent                              # .../direct_data_streaming

PROC_DIR   = _DS_ROOT / "data" / "processed"
REPORT_DIR = _DS_ROOT / "reports" / "eda"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ── Visual style ──────────────────────────────────────────────────
PALETTE   = "Blues_r"
ACCENT    = "#1F4E79"
HIGHLIGHT = "#C00000"
FIG_DPI   = 150
sns.set_theme(style="whitegrid", font_scale=1.05)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def millions(x, _):
    return f"${x/1e6:,.0f}M"

def save(fig, name):
    path = REPORT_DIR / name
    fig.savefig(path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    logging.info(f"  Saved → {path}")


# ─────────────────────────────────────────────
# 1. COST DISTRIBUTION BY INCIDENT TYPE
# ─────────────────────────────────────────────

def plot_cost_by_incident(df):
    logging.info("Plot 1: Cost distribution by incident type…")

    if "incidenttype" not in df.columns:
        logging.warning("  'incidenttype' not found – skipping")
        return

    top_types = (
        df.groupby("incidenttype")["total_obligated_amount"]
        .sum()
        .nlargest(12)
        .index
    )
    sub = df[df["incidenttype"].isin(top_types)].copy()

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Total obligations by type
    totals = (
        sub.groupby("incidenttype")["total_obligated_amount"]
        .sum()
        .sort_values(ascending=True)
    )
    totals.plot(kind="barh", ax=axes[0], color=ACCENT, edgecolor="white")
    axes[0].set_title("Total Obligated Amount by Incident Type", fontweight="bold")
    axes[0].set_xlabel("Total Obligated ($)")
    axes[0].xaxis.set_major_formatter(mticker.FuncFormatter(millions))
    axes[0].set_ylabel("")

    # Box-plot distribution
    order = (
        sub.groupby("incidenttype")["total_obligated_amount"]
        .median()
        .sort_values(ascending=False)
        .index
    )
    sns.boxplot(
        data=sub,
        x="total_obligated_amount",
        y="incidenttype",
        order=order,
        palette="Blues",
        flierprops=dict(marker="o", markersize=3, alpha=0.4),
        ax=axes[1],
    )
    axes[1].set_title("Cost Distribution per Incident Type", fontweight="bold")
    axes[1].set_xlabel("Obligated Amount per Disaster ($)")
    axes[1].xaxis.set_major_formatter(mticker.FuncFormatter(millions))
    axes[1].set_ylabel("")

    fig.suptitle("Recovery Cost by Incident Type", fontsize=14, fontweight="bold", y=1.01)
    save(fig, "cost_distribution.png")


# ─────────────────────────────────────────────
# 2. GEOGRAPHIC FUNDING PATTERNS
# ─────────────────────────────────────────────

def plot_geographic(df):
    logging.info("Plot 2: Geographic funding patterns…")

    if "state" not in df.columns:
        logging.warning("  'state' not found – skipping")
        return

    state_totals = (
        df.groupby("state")["total_obligated_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(25)
    )

    fig, ax = plt.subplots(figsize=(14, 7))
    colors = [HIGHLIGHT if v == state_totals.max() else ACCENT for v in state_totals]
    state_totals.plot(kind="bar", ax=ax, color=colors, edgecolor="white", width=0.8)

    ax.set_title("Top 25 States by Total Federal Obligated Amount", fontweight="bold", fontsize=13)
    ax.set_xlabel("State")
    ax.set_ylabel("Total Obligated ($)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions))
    ax.tick_params(axis="x", rotation=45)

    # Annotate the top bar
    top_state = state_totals.idxmax()
    top_val   = state_totals.max()
    ax.annotate(
        f"{top_state}: {top_val/1e9:.1f}B",
        xy=(0, top_val),
        xytext=(3, top_val * 0.95),
        fontsize=9,
        color=HIGHLIGHT,
        fontweight="bold",
    )

    save(fig, "geographic_funding.png")


# ─────────────────────────────────────────────
# 3. DURATION vs COST
# ─────────────────────────────────────────────

def plot_duration_vs_cost(df):
    logging.info("Plot 3: Incident duration vs recovery cost…")

    if "incident_duration_days" not in df.columns:
        logging.warning("  'incident_duration_days' not found – skipping")
        return

    sub = df[
        (df["incident_duration_days"] > 0) &
        (df["total_obligated_amount"]  > 0)
    ].copy()

    sub["log_cost"]     = np.log10(sub["total_obligated_amount"])
    sub["log_duration"] = np.log10(sub["incident_duration_days"].clip(lower=1))

    fig, ax = plt.subplots(figsize=(10, 7))

    if "incidenttype" in sub.columns:
        top_types = sub["incidenttype"].value_counts().head(6).index
        palette   = sns.color_palette("tab10", n_colors=len(top_types))
        for i, t in enumerate(top_types):
            mask = sub["incidenttype"] == t
            ax.scatter(
                sub.loc[mask, "log_duration"],
                sub.loc[mask, "log_cost"],
                label=t,
                alpha=0.55,
                s=30,
                color=palette[i],
            )
        ax.legend(title="Incident Type", fontsize=8, loc="upper left")
    else:
        ax.scatter(sub["log_duration"], sub["log_cost"], alpha=0.4, s=25, color=ACCENT)

    # Trend line
    z = np.polyfit(sub["log_duration"], sub["log_cost"], 1)
    p = np.poly1d(z)
    xline = np.linspace(sub["log_duration"].min(), sub["log_duration"].max(), 200)
    ax.plot(xline, p(xline), color=HIGHLIGHT, lw=2, ls="--", label="Trend")

    corr = sub[["log_duration", "log_cost"]].corr().iloc[0, 1]
    ax.set_title(
        f"Incident Duration vs Recovery Cost  (r = {corr:.2f})",
        fontweight="bold",
    )
    ax.set_xlabel("Log₁₀ Incident Duration (days)")
    ax.set_ylabel("Log₁₀ Obligated Amount ($)")
    ax.legend(fontsize=8)

    save(fig, "duration_vs_cost.png")


# ─────────────────────────────────────────────
# 4. COST TREND OVER TIME
# ─────────────────────────────────────────────

def plot_cost_over_time(df):
    logging.info("Plot 4: Cost trend over time…")

    if "declaration_year" not in df.columns:
        logging.warning("  'declaration_year' not found – skipping")
        return

    annual = (
        df.groupby("declaration_year")
        .agg(
            total_obligated=("total_obligated_amount", "sum"),
            disaster_count=("disasternumber", "nunique"),
        )
        .reset_index()
    )
    annual = annual[annual["declaration_year"] >= 1990]

    fig, ax1 = plt.subplots(figsize=(14, 6))

    ax1.bar(
        annual["declaration_year"],
        annual["total_obligated"] / 1e9,
        color=ACCENT,
        alpha=0.8,
        label="Total Obligated ($B)",
    )
    ax1.set_ylabel("Total Obligated ($B)", color=ACCENT)
    ax1.tick_params(axis="y", labelcolor=ACCENT)
    ax1.set_xlabel("Year")

    ax2 = ax1.twinx()
    ax2.plot(
        annual["declaration_year"],
        annual["disaster_count"],
        color=HIGHLIGHT,
        marker="o",
        ms=4,
        lw=1.8,
        label="No. of Disasters",
    )
    ax2.set_ylabel("Number of Disaster Declarations", color=HIGHLIGHT)
    ax2.tick_params(axis="y", labelcolor=HIGHLIGHT)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

    ax1.set_title("Annual Disaster Recovery Obligations & Declaration Counts", fontweight="bold")
    save(fig, "cost_over_time.png")


# ─────────────────────────────────────────────
# 5. OUTLIER ANALYSIS
# ─────────────────────────────────────────────

def plot_outliers(df):
    logging.info("Plot 5: Outlier analysis…")

    if "incidenttype" not in df.columns:
        logging.warning("  'incidenttype' not found – skipping")
        return

    top_types = df["incidenttype"].value_counts().head(8).index
    sub = df[df["incidenttype"].isin(top_types)].copy()

    fig, ax = plt.subplots(figsize=(13, 6))
    sns.boxplot(
        data=sub,
        x="incidenttype",
        y="total_obligated_amount",
        order=sub.groupby("incidenttype")["total_obligated_amount"].median().sort_values(ascending=False).index,
        palette="Blues",
        flierprops=dict(marker="D", markersize=4, alpha=0.4, color=HIGHLIGHT),
        ax=ax,
    )
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_title("Outlier Detection: Recovery Cost by Incident Type (log scale)", fontweight="bold")
    ax.set_xlabel("Incident Type")
    ax.set_ylabel("Obligated Amount (log $)")
    ax.tick_params(axis="x", rotation=30)

    save(fig, "outlier_analysis.png")


# ─────────────────────────────────────────────
# 6. CORRELATION HEATMAP
# ─────────────────────────────────────────────

def plot_correlation(df):
    logging.info("Plot 6: Feature correlation heatmap…")

    numeric_cols = [
        "total_obligated_amount",
        "log_total_obligated_amount",
        "incident_duration_days",
        "days_to_declaration",
        "incident_severity_score",
        "state_disaster_frequency",
        "project_count",
        "avg_project_cost",
        "declaration_year",
        "declaration_month",
    ]

    available = [c for c in numeric_cols if c in df.columns]
    if len(available) < 3:
        logging.warning("  Too few numeric columns for heatmap – skipping")
        return

    corr = df[available].corr()

    fig, ax = plt.subplots(figsize=(12, 9))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(
        corr,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="Blues",
        linewidths=0.5,
        ax=ax,
        annot_kws={"size": 9},
    )
    ax.set_title("Feature Correlation Matrix", fontweight="bold", fontsize=13)
    plt.xticks(rotation=35, ha="right")

    save(fig, "correlation_heatmap.png")


# ─────────────────────────────────────────────
# 7. SUMMARY STATISTICS CSV
# ─────────────────────────────────────────────

def save_summary(df):
    logging.info("Saving summary statistics…")

    numeric = df.select_dtypes(include=[np.number])
    summary = numeric.describe(percentiles=[0.25, 0.5, 0.75, 0.90, 0.95]).T
    summary["skewness"] = numeric.skew()
    summary["kurtosis"] = numeric.kurtosis()

    path = REPORT_DIR / "eda_summary.csv"
    summary.to_csv(path)
    logging.info(f"  Saved → {path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logging.info("Loading feature set…")
    df = pd.read_parquet(PROC_DIR / "features.parquet")
    logging.info(f"  Shape: {df.shape}")

    # Ensure log target exists
    if "log_total_obligated_amount" not in df.columns and "total_obligated_amount" in df.columns:
        df["log_total_obligated_amount"] = np.log1p(df["total_obligated_amount"].fillna(0))

    plot_cost_by_incident(df)
    plot_geographic(df)
    plot_duration_vs_cost(df)
    plot_cost_over_time(df)
    plot_outliers(df)
    plot_correlation(df)
    save_summary(df)

    logging.info("===== STEP 3 EDA COMPLETE =====")
    logging.info(f"All outputs saved to: {REPORT_DIR.resolve()}")


if __name__ == "__main__":
    main()