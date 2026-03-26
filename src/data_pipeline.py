"""
ETL Data Pipeline: Medicare Physician Analytics
Extracts, transforms, validates, and loads provider data into SQLite.
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from typing import Optional

# -- Path helpers ---------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "medicare_providers.csv")
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")

STATE_REGION = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast", "RI": "Northeast",
    "VT": "Northeast",
    "AL": "Southeast", "AR": "Southeast", "DE": "Southeast", "FL": "Southeast",
    "GA": "Southeast", "KY": "Southeast", "LA": "Southeast", "MD": "Southeast",
    "MS": "Southeast", "NC": "Southeast", "SC": "Southeast", "TN": "Southeast",
    "VA": "Southeast", "WV": "Southeast",
    "IL": "Midwest", "IN": "Midwest", "IA": "Midwest", "KS": "Midwest",
    "MI": "Midwest", "MN": "Midwest", "MO": "Midwest", "NE": "Midwest",
    "ND": "Midwest", "OH": "Midwest", "SD": "Midwest", "WI": "Midwest",
    "AZ": "Southwest", "NM": "Southwest", "OK": "Southwest", "TX": "Southwest",
    "AK": "West", "CA": "West", "CO": "West", "HI": "West", "ID": "West",
    "MT": "West", "NV": "West", "OR": "West", "UT": "West", "WA": "West",
    "WY": "West",
}


# ==============================================================================
# EXTRACT
# ==============================================================================

def extract(path: str = RAW_DATA_PATH, chunksize: int = 10_000) -> pd.DataFrame:
    """
    Load CSV in chunks to handle large files efficiently.

    Args:
        path: Path to the raw CSV file.
        chunksize: Number of rows per chunk.

    Returns:
        Full DataFrame after loading all chunks.
    """
    print("\n-- EXTRACT -----------------------------------------------------")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Raw data not found at {path}. Run generate_data.py first."
        )

    chunks = []
    total_rows = 0
    try:
        for i, chunk in enumerate(
            pd.read_csv(path, chunksize=chunksize, encoding="utf-8",
                        low_memory=False, dtype=str)
        ):
            chunks.append(chunk)
            total_rows += len(chunk)
            if (i + 1) % 5 == 0:
                print(f"  Loaded {total_rows:,} rows so far...")
    except UnicodeDecodeError:
        print("  UTF-8 failed -- retrying with latin-1 encoding...")
        chunks = []
        total_rows = 0
        for chunk in pd.read_csv(path, chunksize=chunksize, encoding="latin-1",
                                  low_memory=False, dtype=str):
            chunks.append(chunk)
            total_rows += len(chunk)

    df = pd.concat(chunks, ignore_index=True)
    print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")
    return df


# ==============================================================================
# TRANSFORM
# ==============================================================================

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich the raw provider DataFrame.

    Steps:
      1. Drop rows missing critical fields.
      2. Cast numeric columns.
      3. Standardize specialty names.
      4. Derive features (cost_per_patient, charge ratio, etc.).
      5. Compute per-specialty z-scores and outlier flags.
      6. Add volume tier and region.

    Args:
        df: Raw DataFrame from extract().

    Returns:
        Transformed DataFrame ready for validation and loading.
    """
    print("\n-- TRANSFORM ---------------------------------------------------")
    original_len = len(df)

    # -- 1. Drop rows with missing critical fields --------------------------
    critical_cols = ["npi", "provider_specialty", "avg_medicare_payment"]
    df = df.dropna(subset=critical_cols)
    print(f"  Dropped {original_len - len(df):,} rows with missing critical fields")

    # -- 2. Cast numeric columns --------------------------------------------
    numeric_cols = [
        "total_beneficiaries", "total_services",
        "avg_submitted_charge", "avg_medicare_payment", "avg_beneficiary_age"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["avg_medicare_payment", "total_beneficiaries"])
    df = df[df["avg_medicare_payment"] > 0]
    df = df[df["total_beneficiaries"] > 0]
    print(f"  Rows after numeric cleaning: {len(df):,}")

    # -- 3. Standardize specialty names ------------------------------------
    df["provider_specialty"] = df["provider_specialty"].str.strip().str.title()

    # -- 4. Derived features ------------------------------------------------
    df["cost_per_patient"] = (
        df["avg_medicare_payment"] / df["total_beneficiaries"]
    ).round(4)

    df["charge_to_payment_ratio"] = (
        df["avg_submitted_charge"] / df["avg_medicare_payment"].replace(0, np.nan)
    ).round(4)

    # Volume tier (quartile-based per full dataset)
    try:
        df["volume_tier"] = pd.qcut(
            df["total_beneficiaries"], q=4,
            labels=["Low", "Medium", "High", "Very High"],
            duplicates="drop"
        )
    except ValueError:
        df["volume_tier"] = "Medium"
    df["volume_tier"] = df["volume_tier"].astype(str)

    # -- 5. Per-specialty z-scores ------------------------------------------
    specialty_stats = (
        df.groupby("provider_specialty")["avg_medicare_payment"]
        .agg(specialty_mean="mean", specialty_std="std", specialty_p75=lambda x: x.quantile(0.75))
        .reset_index()
    )
    df = df.merge(specialty_stats, on="provider_specialty", how="left")

    df["cost_z_score"] = (
        (df["avg_medicare_payment"] - df["specialty_mean"])
        / df["specialty_std"].replace(0, np.nan)
    ).round(4)

    df["is_high_cost"] = (
        df["avg_medicare_payment"] > df["specialty_p75"]
    ).astype(int)

    df["is_outlier"] = (df["cost_z_score"].abs() > 2).astype(int)

    # -- 6. Region ----------------------------------------------------------
    df["region"] = df["provider_state"].map(STATE_REGION).fillna("Unknown")

    # -- 7. Derived full name -----------------------------------------------
    if "provider_last_name" in df.columns and "provider_first_name" in df.columns:
        df["provider_name"] = (
            "Dr. " + df["provider_last_name"].str.title()
            + " " + df["provider_first_name"].str.title()
        )
    else:
        df["provider_name"] = "Dr. Unknown"

    print(f"  Derived features added")
    print(f"  Outlier providers flagged: {df['is_outlier'].sum():,}")
    print(f"  High-cost providers: {df['is_high_cost'].sum():,}")
    print(f"  Regions: {df['region'].value_counts().to_dict()}")
    print(f"  Final shape after transform: {df.shape}")
    return df


# ==============================================================================
# VALIDATE
# ==============================================================================

def validate(df: pd.DataFrame) -> bool:
    """
    Run data quality checks and print a validation report.

    Args:
        df: Transformed DataFrame.

    Returns:
        True if all checks pass, False otherwise.
    """
    print("\n-- VALIDATE ----------------------------------------------------")
    issues = []

    # Check 1: Duplicate NPI + procedure combinations
    dup_count = df.duplicated(subset=["npi", "hcpcs_code"]).sum()
    if dup_count > 0:
        issues.append(f"  WARNING: {dup_count:,} duplicate (npi, hcpcs_code) pairs")
    else:
        print("  OK No duplicate NPI+procedure pairs")

    # Check 2: Negative costs
    neg_cost = (df["avg_medicare_payment"] < 0).sum()
    if neg_cost > 0:
        issues.append(f"  ERROR: {neg_cost:,} rows with negative payments")
    else:
        print("  OK All payment values are positive")

    # Check 3: Z-score sanity
    z_mean = df["cost_z_score"].mean()
    z_std = df["cost_z_score"].std()
    if abs(z_mean) > 0.5:
        issues.append(f"  WARNING: Z-score mean is {z_mean:.3f} (expected ~0)")
    else:
        print(f"  OK Z-score mean ~= {z_mean:.3f} (expected ~0)")

    # Check 4: Cost range sanity
    max_cost = df["avg_medicare_payment"].max()
    if max_cost > 1_000_000:
        issues.append(f"  WARNING: Max payment ${max_cost:,.2f} seems unrealistic")
    else:
        print(f"  OK Max payment ${max_cost:,.2f} - within realistic range")

    # Check 5: State coverage
    n_states = df["provider_state"].nunique()
    print(f"  OK States covered: {n_states}/50")

    # Check 6: All specialties present
    n_specialties = df["provider_specialty"].nunique()
    print(f"  OK Specialties: {n_specialties}")

    # Report
    if issues:
        print("\n  Data Quality Issues:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("  OK All validation checks passed")

    print(f"\n  Validation Report:")
    print(f"    Total rows:        {len(df):,}")
    print(f"    Unique providers:  {df['npi'].nunique():,}")
    print(f"    Outlier rows:      {df['is_outlier'].sum():,}")
    print(f"    Missing z-scores:  {df['cost_z_score'].isna().sum():,}")
    return len(issues) == 0


# ==============================================================================
# LOAD
# ==============================================================================

def _create_tables(conn: sqlite3.Connection) -> None:
    """Create the four SQLite tables (drop and recreate if they exist)."""
    cursor = conn.cursor()
    cursor.executescript("""
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS quality_metrics;
        DROP TABLE IF EXISTS payments;
        DROP TABLE IF EXISTS procedures;
        DROP TABLE IF EXISTS providers;

        CREATE TABLE providers (
            npi          TEXT PRIMARY KEY,
            provider_name TEXT,
            specialty    TEXT,
            state        TEXT,
            city         TEXT,
            region       TEXT,
            total_patients INTEGER,
            volume_tier  TEXT
        );

        CREATE TABLE procedures (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            npi              TEXT,
            hcpcs_code       TEXT,
            hcpcs_description TEXT,
            total_services   INTEGER,
            FOREIGN KEY (npi) REFERENCES providers(npi)
        );

        CREATE TABLE payments (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            npi                     TEXT,
            hcpcs_code              TEXT,
            avg_submitted_charge    REAL,
            avg_medicare_payment    REAL,
            cost_per_patient        REAL,
            charge_to_payment_ratio REAL,
            cost_z_score            REAL,
            is_outlier              INTEGER,
            FOREIGN KEY (npi) REFERENCES providers(npi)
        );

        CREATE TABLE quality_metrics (
            npi                TEXT PRIMARY KEY,
            procedure_diversity INTEGER,
            avg_cost_z_score   REAL,
            cluster_id         INTEGER,
            cluster_name       TEXT,
            performance_tier   TEXT,
            FOREIGN KEY (npi) REFERENCES providers(npi)
        );
    """)
    conn.commit()


def load(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    """
    Write the transformed DataFrame into the SQLite database.

    Args:
        df: Cleaned DataFrame.
        db_path: Path to the SQLite database file.
    """
    print("\n-- LOAD --------------------------------------------------------")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    _create_tables(conn)

    # -- providers ----------------------------------------------------------
    providers_df = (
        df.groupby("npi")
        .agg(
            provider_name=("provider_name", "first"),
            specialty=("provider_specialty", "first"),
            state=("provider_state", "first"),
            city=("provider_city", "first"),
            region=("region", "first"),
            total_patients=("total_beneficiaries", "sum"),
            volume_tier=("volume_tier", "first"),
        )
        .reset_index()
    )
    providers_df.to_sql("providers", conn, if_exists="append", index=False)
    print(f"  Inserted {len(providers_df):,} rows -> providers")

    # -- procedures --------------------------------------------------------
    proc_cols = ["npi", "hcpcs_code", "hcpcs_description", "total_services"]
    available = [c for c in proc_cols if c in df.columns]
    procedures_df = df[available].copy()
    procedures_df.to_sql("procedures", conn, if_exists="append", index=False)
    print(f"  Inserted {len(procedures_df):,} rows -> procedures")

    # -- payments ----------------------------------------------------------
    pay_cols = [
        "npi", "hcpcs_code", "avg_submitted_charge", "avg_medicare_payment",
        "cost_per_patient", "charge_to_payment_ratio", "cost_z_score", "is_outlier"
    ]
    available = [c for c in pay_cols if c in df.columns]
    payments_df = df[available].copy()
    payments_df.to_sql("payments", conn, if_exists="append", index=False)
    print(f"  Inserted {len(payments_df):,} rows -> payments")

    # -- quality_metrics (initial -- will be updated by ML pipeline) --------
    qm_df = (
        df.groupby("npi")
        .agg(
            procedure_diversity=("hcpcs_code", "nunique"),
            avg_cost_z_score=("cost_z_score", "mean"),
        )
        .reset_index()
    )
    qm_df["cluster_id"] = None
    qm_df["cluster_name"] = None
    qm_df["performance_tier"] = None
    qm_df.to_sql("quality_metrics", conn, if_exists="append", index=False)
    print(f"  Inserted {len(qm_df):,} rows -> quality_metrics")

    conn.close()
    print(f"\n  Database saved to: {db_path}")


# ==============================================================================
# MAIN
# ==============================================================================

def run_pipeline(raw_path: str = RAW_DATA_PATH, db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Execute the full ETL pipeline end-to-end.

    Args:
        raw_path: Path to the raw CSV data.
        db_path: Path for the output SQLite database.

    Returns:
        The final transformed DataFrame.
    """
    print("=" * 60)
    print("  Medicare Physician Analytics -- ETL Pipeline")
    print("=" * 60)

    df_raw = extract(raw_path)
    df_clean = transform(df_raw)
    validate(df_clean)
    load(df_clean, db_path)

    print("\n" + "=" * 60)
    print("  ETL Pipeline complete")
    print("=" * 60)
    return df_clean


if __name__ == "__main__":
    run_pipeline()
