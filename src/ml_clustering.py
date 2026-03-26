"""
ML Clustering: Medicare Physician Analytics
K-Means clustering of physicians by practice patterns + SHAP interpretability.
"""

import os
import sqlite3
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    print("  SHAP not available -- skipping SHAP analysis")

warnings.filterwarnings("ignore")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "clusters")

FEATURES = [
    "cost_per_patient",
    "total_patients",
    "procedure_diversity",
    "avg_cost_z_score",
    "avg_charge_ratio",
]

CLUSTER_NAMES_TEMPLATE = {
    "high_cost_low_vol":  "Premium Specialists",
    "low_cost_high_vol":  "Community Providers",
    "extreme_z":          "Outlier Billers",
    "average":            "Standard Practice",
}


# ==============================================================================
# Data Loading
# ==============================================================================

def load_features(db_path: str) -> pd.DataFrame:
    """
    Build the provider-level feature matrix for clustering.

    Args:
        db_path: Path to SQLite database.

    Returns:
        DataFrame indexed by npi with clustering features.
    """
    conn = sqlite3.connect(db_path)

    providers = pd.read_sql("SELECT * FROM providers", conn)
    payments = pd.read_sql("SELECT * FROM payments", conn)
    quality = pd.read_sql("SELECT * FROM quality_metrics", conn)
    conn.close()

    # Aggregate payments to provider level
    pay_agg = (
        payments.groupby("npi")
        .agg(
            avg_cost_z_score=("cost_z_score", "mean"),
            avg_charge_ratio=("charge_to_payment_ratio", "mean"),
            avg_payment=("avg_medicare_payment", "mean"),
            avg_cost_per_patient=("cost_per_patient", "mean"),
        )
        .reset_index()
    )

    df = (
        providers
        .merge(pay_agg, on="npi", how="left")
        .merge(quality[["npi", "procedure_diversity"]], on="npi", how="left")
    )

    df = df.rename(columns={
        "total_patients": "total_patients",
        "avg_cost_per_patient": "cost_per_patient",
    })

    print(f"  Feature matrix: {len(df):,} providers × {len(FEATURES)} features")
    return df


# ==============================================================================
# Preprocessing
# ==============================================================================

def preprocess(df: pd.DataFrame) -> tuple:
    """
    Scale features and impute missing values.

    Args:
        df: Provider-level DataFrame.

    Returns:
        Tuple of (X_scaled, feature_df, scaler, imputer).
    """
    feature_df = df[FEATURES].copy()

    imputer = SimpleImputer(strategy="median")
    X_imputed = imputer.fit_transform(feature_df)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_imputed)

    return X_scaled, feature_df, scaler, imputer


# ==============================================================================
# Optimal K search
# ==============================================================================

def find_optimal_k(X: np.ndarray, k_range: range = range(2, 9),
                   output_dir: str = OUTPUT_DIR) -> int:
    """
    Test k=2..8, plot elbow + silhouette, return optimal k.

    Args:
        X: Scaled feature array.
        k_range: Range of k values to test.
        output_dir: Directory for elbow curve plot.

    Returns:
        Optimal k value.
    """
    print("  Finding optimal k (using 3k-row subsample for speed)...")
    # Subsample for elbow search to keep wall time reasonable
    n_sub = min(3_000, len(X))
    idx_sub = np.random.RandomState(42).choice(len(X), n_sub, replace=False)
    X_sub = X[idx_sub]

    inertias = []
    silhouette_scores = []

    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=3, max_iter=50)
        labels = km.fit_predict(X_sub)
        inertias.append(km.inertia_)
        # Use a further subsample for silhouette (O(n^2) computation)
        sil_n = min(1_000, n_sub)
        sil_idx = np.random.RandomState(42).choice(n_sub, sil_n, replace=False)
        sil = silhouette_score(X_sub[sil_idx], labels[sil_idx])
        silhouette_scores.append(sil)
        print(f"    k={k}: inertia={km.inertia_:.0f}, silhouette={sil:.4f}")

    # Elbow curve plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    ks = list(k_range)
    ax1.plot(ks, inertias, "o-", color="#2196F3", linewidth=2, markersize=7)
    ax1.set_title("Elbow Curve -- Inertia")
    ax1.set_xlabel("Number of Clusters (k)")
    ax1.set_ylabel("Inertia (WCSS)")
    ax1.grid(True, alpha=0.3)

    ax2.plot(ks, silhouette_scores, "s-", color="#4CAF50", linewidth=2, markersize=7)
    ax2.set_title("Silhouette Score by k")
    ax2.set_xlabel("Number of Clusters (k)")
    ax2.set_ylabel("Silhouette Score")
    ax2.grid(True, alpha=0.3)

    # Highlight best k
    best_k = ks[np.argmax(silhouette_scores)]
    ax2.axvline(best_k, color="#F44336", linestyle="--",
                label=f"Best k={best_k}")
    ax2.legend()

    plt.suptitle("K-Means Cluster Selection", fontsize=13, y=1.01)
    plt.tight_layout()
    path = os.path.join(output_dir, "elbow_curve.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Elbow curve saved -> {path}")
    return best_k


# ==============================================================================
# Cluster Naming
# ==============================================================================

def name_clusters(df_clustered: pd.DataFrame, n_clusters: int) -> dict:
    """
    Automatically name clusters based on their feature centroids.

    Logic:
      - Highest avg_cost_z_score  -> "Outlier Billers"
      - Highest cost, lowest vol  -> "Premium Specialists"
      - Lowest cost, highest vol  -> "Community Providers"
      - Remaining                 -> "Standard Practice"

    Args:
        df_clustered: DataFrame with cluster_id column added.
        n_clusters: Number of clusters.

    Returns:
        Dict mapping cluster_id (int) -> cluster_name (str).
    """
    summary = (
        df_clustered.groupby("cluster_id")[FEATURES]
        .mean()
        .reset_index()
    )

    names = {}
    used = set()

    # Outlier Billers: highest absolute avg_cost_z_score
    if "avg_cost_z_score" in summary.columns:
        ob_id = int(summary.loc[summary["avg_cost_z_score"].abs().idxmax(), "cluster_id"])
        names[ob_id] = "Outlier Billers"
        used.add(ob_id)

    remaining = summary[~summary["cluster_id"].isin(used)]

    # Premium Specialists: highest cost_per_patient
    if "cost_per_patient" in remaining.columns and len(remaining) > 0:
        ps_id = int(remaining.loc[remaining["cost_per_patient"].idxmax(), "cluster_id"])
        names[ps_id] = "Premium Specialists"
        used.add(ps_id)

    remaining = summary[~summary["cluster_id"].isin(used)]

    # Community Providers: highest total_patients (high volume)
    if "total_patients" in remaining.columns and len(remaining) > 0:
        cp_id = int(remaining.loc[remaining["total_patients"].idxmax(), "cluster_id"])
        names[cp_id] = "Community Providers"
        used.add(cp_id)

    # Remainder -> Standard Practice
    for cid in summary["cluster_id"].tolist():
        if cid not in names:
            names[cid] = "Standard Practice"

    return names


# ==============================================================================
# SHAP Analysis
# ==============================================================================

def run_shap(X: np.ndarray, labels: np.ndarray,
             feature_names: list, output_dir: str) -> None:
    """
    Train a Random Forest on cluster labels and compute SHAP values.

    Args:
        X: Feature array.
        labels: Cluster label array.
        feature_names: List of feature names.
        output_dir: Directory for SHAP plot.
    """
    if not SHAP_AVAILABLE:
        return

    print("  Running SHAP analysis...")
    # Sample for speed - keep small to avoid long runtimes
    n_sample = min(500, len(X))
    idx = np.random.RandomState(42).choice(len(X), n_sample, replace=False)
    X_s, y_s = X[idx], labels[idx]

    rf = RandomForestClassifier(n_estimators=30, max_depth=6, random_state=42, n_jobs=-1)
    rf.fit(X_s, y_s)

    explainer = shap.TreeExplainer(rf)
    shap_values = explainer.shap_values(X_s)

    fig = plt.figure(figsize=(10, 6))
    shap.summary_plot(
        shap_values if isinstance(shap_values, np.ndarray) else shap_values[0],
        X_s,
        feature_names=feature_names,
        show=False,
        plot_type="bar",
    )
    plt.title("SHAP Feature Importance for Cluster Prediction", pad=12)
    plt.tight_layout()
    path = os.path.join(output_dir, "shap_summary.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    SHAP summary saved -> {path}")


# ==============================================================================
# Visualizations
# ==============================================================================

def plot_clusters(df_clustered: pd.DataFrame, X_scaled: np.ndarray,
                  cluster_names: dict, output_dir: str) -> None:
    """2D PCA scatter plot of clusters."""
    print("  Generating cluster visualization...")

    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(X_scaled)
    df_plot = df_clustered.copy()
    df_plot["pc1"] = coords[:, 0]
    df_plot["pc2"] = coords[:, 1]
    df_plot["cluster_label"] = df_plot["cluster_id"].map(cluster_names)

    fig, ax = plt.subplots(figsize=(10, 7))
    palette = {name: c for name, c in zip(
        cluster_names.values(),
        ["#2196F3", "#4CAF50", "#FF9800", "#F44336"]
    )}

    for name, grp in df_plot.groupby("cluster_label"):
        ax.scatter(grp["pc1"], grp["pc2"], s=15, alpha=0.4,
                   label=f"{name} (n={len(grp):,})",
                   color=palette.get(name, "#9E9E9E"))

    # Cluster centroids in PCA space
    for cid, name in cluster_names.items():
        mask = df_plot["cluster_id"] == cid
        cx, cy = df_plot.loc[mask, "pc1"].mean(), df_plot.loc[mask, "pc2"].mean()
        ax.scatter(cx, cy, s=200, marker="*", color="black", zorder=5)
        ax.annotate(name, (cx, cy), textcoords="offset points",
                    xytext=(5, 5), fontsize=8, fontweight="bold")

    var_exp = pca.explained_variance_ratio_
    ax.set_title(
        f"Provider Clusters -- PCA Projection\n"
        f"(PC1={var_exp[0]:.1%}, PC2={var_exp[1]:.1%} variance explained)",
        pad=12
    )
    ax.set_xlabel("Principal Component 1")
    ax.set_ylabel("Principal Component 2")
    ax.legend(title="Cluster", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=9)
    plt.tight_layout()
    path = os.path.join(output_dir, "cluster_visualization.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"    Cluster visualization saved -> {path}")


# ==============================================================================
# Database Update
# ==============================================================================

def update_quality_metrics(df_clustered: pd.DataFrame,
                            cluster_names: dict,
                            db_path: str) -> None:
    """
    Write cluster assignments and performance tiers back to quality_metrics table.

    Args:
        df_clustered: DataFrame with cluster_id and npi columns.
        cluster_names: Dict mapping cluster_id -> cluster_name.
        db_path: SQLite database path.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Performance tier based on avg_cost_z_score
    def perf_tier(z: float) -> str:
        if pd.isna(z):
            return "Unknown"
        if z > 1.5:
            return "High Cost"
        elif z > 0.5:
            return "Above Average"
        elif z > -0.5:
            return "Average"
        elif z > -1.5:
            return "Below Average"
        else:
            return "Low Cost"

    # Build update DataFrame
    update_df = df_clustered[["npi", "cluster_id"]].copy()
    update_df["cluster_name"] = update_df["cluster_id"].map(cluster_names).fillna("Unknown")

    z_col = "avg_cost_z_score" if "avg_cost_z_score" in df_clustered.columns else None
    if z_col:
        update_df["performance_tier"] = df_clustered[z_col].map(perf_tier)
    else:
        update_df["performance_tier"] = "Unknown"

    # Read existing quality_metrics, merge cluster info, and overwrite the table
    existing = pd.read_sql("SELECT npi, procedure_diversity, avg_cost_z_score FROM quality_metrics", conn)
    merged = existing.merge(
        update_df[["npi", "cluster_id", "cluster_name", "performance_tier"]],
        on="npi", how="left"
    )
    cursor.execute("DROP TABLE IF EXISTS quality_metrics")
    conn.commit()
    merged.to_sql("quality_metrics", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()
    print(f"  quality_metrics updated for {len(df_clustered):,} providers (bulk replace)")


# ==============================================================================
# MAIN
# ==============================================================================

def run_clustering(db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR) -> pd.DataFrame:
    """
    Full clustering pipeline: load -> preprocess -> k-search -> KMeans -> name -> SHAP.

    Args:
        db_path: SQLite database path.
        output_dir: Output directory for plots and CSVs.

    Returns:
        DataFrame with cluster assignments.
    """
    print("=" * 60)
    print("  Medicare Physician Analytics -- ML Clustering")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)

    # Load
    df = load_features(db_path)

    # Preprocess
    X_scaled, feature_df, scaler, imputer = preprocess(df)

    # Optimal k
    optimal_k = find_optimal_k(X_scaled, k_range=range(2, 9), output_dir=output_dir)
    print(f"\n  Optimal k selected: {optimal_k}")

    # KMeans
    print(f"  Running KMeans(k={optimal_k})...")
    km = KMeans(n_clusters=optimal_k, random_state=42, n_init=5, max_iter=200)
    labels = km.fit_predict(X_scaled)
    df["cluster_id"] = labels

    # Name clusters
    cluster_names = name_clusters(df, optimal_k)
    df["cluster_name"] = df["cluster_id"].map(cluster_names)
    print(f"\n  Cluster assignments:")
    for cid, name in sorted(cluster_names.items()):
        n = (df["cluster_id"] == cid).sum()
        print(f"    Cluster {cid}: {name} ({n:,} providers)")

    # Cluster summary
    summary = (
        df.groupby(["cluster_id", "cluster_name"])[FEATURES]
        .mean()
        .round(3)
        .reset_index()
    )
    print("\n  Cluster Feature Means:")
    print(summary.to_string(index=False))
    summary_path = os.path.join(output_dir, "cluster_summary.csv")
    summary.to_csv(summary_path, index=False)
    print(f"\n  Cluster summary saved -> {summary_path}")

    # SHAP
    run_shap(X_scaled, labels, FEATURES, output_dir)

    # Visualize
    plot_clusters(df, X_scaled, cluster_names, output_dir)

    # Persist to DB
    update_quality_metrics(df, cluster_names, db_path)

    print("\n" + "=" * 60)
    print("  ML Clustering complete")
    print("=" * 60)
    return df


if __name__ == "__main__":
    run_clustering()
