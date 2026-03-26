"""
Exploratory Data Analysis: Medicare Physician Analytics
Generates 6 publication-quality plots from the processed database.
"""

import os
import sqlite3
import warnings

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for servers
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "eda")

# -- Style ----------------------------------------------------------------------
sns.set_theme(style="whitegrid", palette="husl")
plt.rcParams.update({
    "figure.dpi": 120,
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})

REGION_COLORS = {
    "Northeast": "#2196F3",
    "Southeast": "#4CAF50",
    "Midwest":   "#FF9800",
    "Southwest": "#9C27B0",
    "West":      "#F44336",
    "Unknown":   "#9E9E9E",
}


def load_data(db_path: str) -> dict:
    """
    Load all tables from SQLite into DataFrames.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Dict with keys 'providers', 'payments', 'procedures', 'quality'.
    """
    conn = sqlite3.connect(db_path)
    data = {
        "providers":  pd.read_sql("SELECT * FROM providers", conn),
        "payments":   pd.read_sql("SELECT * FROM payments", conn),
        "procedures": pd.read_sql("SELECT * FROM procedures", conn),
        "quality":    pd.read_sql("SELECT * FROM quality_metrics", conn),
    }
    conn.close()

    # Merged view
    data["merged"] = data["providers"].merge(data["payments"], on="npi", how="inner")
    print(f"  Loaded {len(data['merged']):,} merged rows for EDA")
    return data


# ==============================================================================
# Plot 1: Cost Distribution by Specialty (box plot)
# ==============================================================================

def plot1_specialty_cost(df: pd.DataFrame, output_dir: str) -> None:
    """Box plot of payment cost by top-10 most variable specialties."""
    print("  Plot 1: Specialty cost distribution...")

    # Top 10 specialties by coefficient of variation
    top10 = (
        df.groupby("specialty")["avg_medicare_payment"]
        .std()
        .nlargest(10)
        .index.tolist()
    )
    subset = df[df["specialty"].isin(top10)].copy()

    # Shorten long names
    subset["specialty"] = subset["specialty"].str.replace("/", "/\n", regex=False)

    fig, ax = plt.subplots(figsize=(14, 7))
    order = (
        subset.groupby("specialty")["avg_medicare_payment"]
        .median()
        .sort_values(ascending=False)
        .index
    )
    sns.boxplot(
        data=subset, x="specialty", y="avg_medicare_payment",
        order=order, palette="Set2",
        hue="is_outlier", dodge=False,
        flierprops=dict(marker="o", markersize=2, alpha=0.4),
        ax=ax,
    )
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Medicare Payment Distribution by Specialty\n(Top 10 Most Variable)", pad=12)
    ax.set_xlabel("Specialty")
    ax.set_ylabel("Avg Medicare Payment ($)")
    ax.tick_params(axis="x", rotation=30)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, ["Normal", "Outlier"], title="Outlier", loc="upper right")
    plt.tight_layout()
    path = os.path.join(output_dir, "specialty_cost_distribution.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# Plot 2: Geographic Cost Variation (bar chart)
# ==============================================================================

def plot2_geographic(df: pd.DataFrame, output_dir: str) -> None:
    """Horizontal bar chart of average cost by state, colored by region."""
    print("  Plot 2: Geographic cost variation...")

    state_avg = (
        df.groupby(["state", "region"])["avg_medicare_payment"]
        .mean()
        .reset_index()
        .rename(columns={"avg_medicare_payment": "avg_cost"})
        .sort_values("avg_cost", ascending=True)
    )

    colors = [REGION_COLORS.get(r, "#9E9E9E") for r in state_avg["region"]]

    fig, ax = plt.subplots(figsize=(10, 14))
    bars = ax.barh(state_avg["state"], state_avg["avg_cost"], color=colors)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Average Medicare Payment by State\n(Colored by Region)", pad=12)
    ax.set_xlabel("Avg Medicare Payment ($)")
    ax.set_ylabel("State")

    # Legend
    from matplotlib.patches import Patch
    legend_elems = [Patch(facecolor=c, label=r) for r, c in REGION_COLORS.items()
                    if r != "Unknown"]
    ax.legend(handles=legend_elems, title="Region", loc="lower right", fontsize=9)

    plt.tight_layout()
    path = os.path.join(output_dir, "geographic_cost_variation.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# Plot 3: Volume vs Cost Scatter
# ==============================================================================

def plot3_volume_cost(df: pd.DataFrame, output_dir: str) -> None:
    """Scatter plot of total_patients vs avg_medicare_payment, colored by specialty cluster."""
    print("  Plot 3: Volume vs cost scatter...")

    # Use top 8 specialties by count for clarity
    top_specs = df["specialty"].value_counts().nlargest(8).index
    subset = df[df["specialty"].isin(top_specs)].sample(
        min(5000, len(df)), random_state=42
    )

    fig, ax = plt.subplots(figsize=(12, 7))
    for spec in top_specs:
        sp_data = subset[subset["specialty"] == spec]
        ax.scatter(
            sp_data["total_patients"],
            sp_data["avg_medicare_payment"],
            s=sp_data["charge_to_payment_ratio"].clip(0, 10) * 5,
            alpha=0.4, label=spec,
        )
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Patient Volume vs Medicare Payment\n(dot size ? charge-to-payment ratio)", pad=12)
    ax.set_xlabel("Total Patients (log scale)")
    ax.set_ylabel("Avg Medicare Payment ($)")
    ax.legend(title="Specialty", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    path = os.path.join(output_dir, "volume_vs_cost.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# Plot 4: Outlier Distribution (histogram of z-scores)
# ==============================================================================

def plot4_outlier_distribution(df: pd.DataFrame, output_dir: str) -> None:
    """Histogram of cost z-scores with outlier threshold lines."""
    print("  Plot 4: Outlier distribution...")

    z_scores = df["cost_z_score"].dropna()
    z_scores = z_scores[z_scores.between(-6, 6)]  # trim extreme tails for display

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(z_scores, bins=80, color="#42A5F5", edgecolor="white", alpha=0.8)
    ax.axvline(-2, color="#F44336", linewidth=2, linestyle="--", label="Outlier threshold (±2)")
    ax.axvline(+2, color="#F44336", linewidth=2, linestyle="--")
    ax.fill_betweenx([0, ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1],
                     -6, -2, alpha=0.08, color="#F44336")
    ax.fill_betweenx([0, ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1],
                     +2, +6, alpha=0.08, color="#F44336")
    ax.set_title("Distribution of Provider Cost Z-Scores\n(Outlier threshold at ±2)", pad=12)
    ax.set_xlabel("Cost Z-Score (within specialty)")
    ax.set_ylabel("Number of Providers")
    ax.legend()

    # Annotations
    n_outliers = (df["is_outlier"] == 1).sum()
    ax.annotate(
        f"Outliers: {n_outliers:,}",
        xy=(3.5, ax.get_ylim()[1] * 0.8 if ax.get_ylim()[1] > 0 else 100),
        fontsize=9, color="#F44336",
    )

    plt.tight_layout()
    path = os.path.join(output_dir, "outlier_distribution.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# Plot 5: Variation Ratio by Specialty (horizontal bar)
# ==============================================================================

def plot5_variation_ratio(df: pd.DataFrame, output_dir: str) -> None:
    """Horizontal bar chart of top-15 specialties by cost variation ratio."""
    print("  Plot 5: Variation ratio by specialty...")

    var_ratio = (
        df.groupby("specialty")["avg_medicare_payment"]
        .agg(lambda x: x.max() / x.min() if x.min() > 0 else np.nan)
        .dropna()
        .nlargest(15)
        .reset_index()
        .rename(columns={"avg_medicare_payment": "variation_ratio"})
        .sort_values("variation_ratio")
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    colors = sns.color_palette("Reds_r", len(var_ratio))
    bars = ax.barh(var_ratio["specialty"], var_ratio["variation_ratio"], color=colors)

    for bar, val in zip(bars, var_ratio["variation_ratio"]):
        ax.text(val + 0.3, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}x", va="center", fontsize=9)

    ax.set_title("Cost Variation Ratio by Specialty\n(max / min payment)", pad=12)
    ax.set_xlabel("Variation Ratio (max / min)")
    ax.set_ylabel("Specialty")
    plt.tight_layout()
    path = os.path.join(output_dir, "variation_ratio.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# Plot 6: Regional Comparison (grouped bar)
# ==============================================================================

def plot6_regional_comparison(df: pd.DataFrame, output_dir: str) -> None:
    """Grouped bar chart comparing top-5 specialties across 4 regions."""
    print("  Plot 6: Regional comparison...")

    top5_specs = df.groupby("specialty")["avg_medicare_payment"].mean().nlargest(5).index.tolist()
    subset = df[df["specialty"].isin(top5_specs) & df["region"].isin(
        ["Northeast", "Southeast", "Midwest", "West"]
    )]

    pivot = (
        subset.groupby(["specialty", "region"])["avg_medicare_payment"]
        .mean()
        .unstack("region")
        .reindex(columns=["Northeast", "Southeast", "Midwest", "West"])
        .fillna(0)
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    pivot.plot(kind="bar", ax=ax,
               color=[REGION_COLORS[r] for r in pivot.columns],
               edgecolor="white", width=0.75)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Average Medicare Payment by Region\n(Top 5 Highest-Cost Specialties)", pad=12)
    ax.set_xlabel("Specialty")
    ax.set_ylabel("Avg Medicare Payment ($)")
    ax.tick_params(axis="x", rotation=30)
    ax.legend(title="Region", bbox_to_anchor=(1.01, 1), loc="upper left")
    plt.tight_layout()
    path = os.path.join(output_dir, "regional_comparison.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Saved -> {path}")


# ==============================================================================
# MAIN
# ==============================================================================

def run_eda(db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR) -> None:
    """
    Execute all 6 EDA plots and print summary statistics.

    Args:
        db_path: Path to the SQLite database.
        output_dir: Directory where PNG plots will be saved.
    """
    print("=" * 60)
    print("  Medicare Physician Analytics -- EDA")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    data = load_data(db_path)
    df = data["merged"]

    # Summary statistics
    print("\n-- Summary Statistics -------------------------------------------")
    print(f"  Total records:       {len(df):,}")
    print(f"  Unique providers:    {df['npi'].nunique():,}")
    print(f"  Specialties:         {df['specialty'].nunique()}")
    print(f"  States:              {df['state'].nunique()}")
    print(f"  Avg payment:         ${df['avg_medicare_payment'].mean():,.2f}")
    print(f"  Median payment:      ${df['avg_medicare_payment'].median():,.2f}")
    print(f"  Payment std dev:     ${df['avg_medicare_payment'].std():,.2f}")
    print(f"  Outlier fraction:    {(df['is_outlier'] == 1).mean():.1%}")
    print()

    plot1_specialty_cost(df, output_dir)
    plot2_geographic(df, output_dir)
    plot3_volume_cost(df, output_dir)
    plot4_outlier_distribution(df, output_dir)
    plot5_variation_ratio(df, output_dir)
    plot6_regional_comparison(df, output_dir)

    print("\n" + "=" * 60)
    print("  EDA complete -- plots saved to:", output_dir)
    print("=" * 60)


if __name__ == "__main__":
    run_eda()
