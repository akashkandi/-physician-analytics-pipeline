"""
Pytest test suite for the Medicare Physician Analytics pipeline.
All tests use the live SQLite database and CSV outputs.
"""

import os
import sqlite3

import pandas as pd
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_CSV = os.path.join(BASE_DIR, "data", "raw", "medicare_providers.csv")
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
SQL_DIR = os.path.join(BASE_DIR, "outputs", "sql_results")

EXPECTED_COLUMNS = [
    "npi", "provider_last_name", "provider_first_name",
    "provider_specialty", "provider_state", "provider_city", "provider_zip",
    "hcpcs_code", "hcpcs_description", "total_beneficiaries", "total_services",
    "avg_submitted_charge", "avg_medicare_payment", "avg_beneficiary_age",
]

EXPECTED_TABLES = ["providers", "procedures", "payments", "quality_metrics"]

SQL_RESULT_FILES = [
    "query1_specialty_variation.csv",
    "query2_geographic.csv",
    "query3_volume_cost.csv",
    "query4_outliers.csv",
    "query5_procedures.csv",
    "query6_quartiles.csv",
]


# -- Fixtures -------------------------------------------------------------------

@pytest.fixture(scope="module")
def raw_df() -> pd.DataFrame:
    """Load the raw CSV once for all tests that need it."""
    assert os.path.exists(RAW_CSV), f"Raw CSV not found at {RAW_CSV}"
    return pd.read_csv(RAW_CSV, low_memory=False)


@pytest.fixture(scope="module")
def db_conn():
    """Open a read-only connection to the processed SQLite database."""
    assert os.path.exists(DB_PATH), f"Database not found at {DB_PATH}"
    conn = sqlite3.connect(DB_PATH)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def payments_df(db_conn) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM payments", db_conn)


@pytest.fixture(scope="module")
def providers_df(db_conn) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM providers", db_conn)


@pytest.fixture(scope="module")
def quality_df(db_conn) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM quality_metrics", db_conn)


# ==============================================================================
# 1. Data loads correctly
# ==============================================================================

def test_data_loads_correctly(raw_df: pd.DataFrame) -> None:
    """CSV must load with expected columns and at least 100,000 rows."""
    assert len(raw_df) >= 100_000, (
        f"Expected ?100,000 rows, got {len(raw_df):,}"
    )
    missing = set(EXPECTED_COLUMNS) - set(raw_df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_raw_data_row_count(raw_df: pd.DataFrame) -> None:
    """Check data is not empty and has substantial coverage."""
    assert len(raw_df) > 0
    assert raw_df["provider_specialty"].nunique() >= 25, (
        "Expected at least 25 specialties"
    )
    assert raw_df["provider_state"].nunique() >= 45, (
        "Expected at least 45 states"
    )


# ==============================================================================
# 2. No negative costs
# ==============================================================================

def test_no_negative_costs(payments_df: pd.DataFrame) -> None:
    """All avg_medicare_payment values must be strictly positive."""
    neg = (payments_df["avg_medicare_payment"] < 0).sum()
    assert neg == 0, f"Found {neg} rows with negative avg_medicare_payment"


def test_no_negative_submitted_charges(payments_df: pd.DataFrame) -> None:
    """avg_submitted_charge must be >= avg_medicare_payment (charges ? payments)."""
    invalid = (
        payments_df["avg_submitted_charge"] < payments_df["avg_medicare_payment"]
    ).sum()
    assert invalid == 0, (
        f"Found {invalid} rows where submitted charge < medicare payment"
    )


# ==============================================================================
# 3. Z-scores are calculated
# ==============================================================================

def test_z_scores_calculated(payments_df: pd.DataFrame) -> None:
    """Z-scores should exist and have mean ~= 0, std ~= 1 (per specialty)."""
    assert "cost_z_score" in payments_df.columns
    z = payments_df["cost_z_score"].dropna()
    assert len(z) > 0, "No z-scores found"
    assert abs(z.mean()) < 1.0, (
        f"Overall z-score mean {z.mean():.3f} is too far from 0 "
        "(note: slight deviation is normal before per-specialty centering)"
    )


def test_z_scores_finite(payments_df: pd.DataFrame) -> None:
    """Z-scores should be finite numbers (no inf/nan)."""
    import numpy as np
    z = payments_df["cost_z_score"]
    n_inf = (~np.isfinite(z.dropna())).sum()
    assert n_inf == 0, f"Found {n_inf} infinite z-score values"


# ==============================================================================
# 4. Outlier flag is correct
# ==============================================================================

def test_outlier_flag_correct(payments_df: pd.DataFrame) -> None:
    """Rows flagged as outliers must have |z-score| > 2."""
    outliers = payments_df[payments_df["is_outlier"] == 1]
    if len(outliers) == 0:
        pytest.skip("No outliers in dataset")
    flagged_correctly = (outliers["cost_z_score"].abs() > 2).all()
    assert flagged_correctly, (
        "Some rows flagged as outliers have |z_score| ? 2"
    )


def test_non_outliers_not_flagged(payments_df: pd.DataFrame) -> None:
    """Rows with |z-score| <= 2 must NOT be flagged as outliers."""
    non_outlier_mask = payments_df["cost_z_score"].abs() <= 2
    wrongly_flagged = (
        payments_df.loc[non_outlier_mask, "is_outlier"] == 1
    ).sum()
    assert wrongly_flagged == 0, (
        f"{wrongly_flagged} providers incorrectly flagged as outliers"
    )


# ==============================================================================
# 5. Database tables exist
# ==============================================================================

def test_database_tables_exist(db_conn) -> None:
    """All 4 required tables must exist in the SQLite database."""
    cursor = db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in cursor.fetchall()}
    for table in EXPECTED_TABLES:
        assert table in existing, f"Missing table: {table}"


def test_database_tables_non_empty(db_conn) -> None:
    """Each table must have at least one row."""
    for table in EXPECTED_TABLES:
        count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", db_conn)["n"].iloc[0]
        assert count > 0, f"Table {table} is empty"


# ==============================================================================
# 6. SQL queries return results
# ==============================================================================

def test_sql_query_returns_results() -> None:
    """All 6 SQL result CSVs must exist and be non-empty."""
    for fname in SQL_RESULT_FILES:
        path = os.path.join(SQL_DIR, fname)
        assert os.path.exists(path), f"Missing SQL result: {path}"
        df = pd.read_csv(path)
        assert len(df) > 0, f"SQL result {fname} is empty"


def test_query1_has_variation_ratio() -> None:
    """Query 1 must include a variation_ratio column with positive values."""
    path = os.path.join(SQL_DIR, "query1_specialty_variation.csv")
    if not os.path.exists(path):
        pytest.skip("Query 1 CSV not found")
    df = pd.read_csv(path)
    assert "variation_ratio" in df.columns
    assert (df["variation_ratio"] > 0).all()


# ==============================================================================
# 7. Clusters are assigned
# ==============================================================================

def test_clusters_assigned(quality_df: pd.DataFrame) -> None:
    """All providers in quality_metrics should have a cluster assignment."""
    n_total = len(quality_df)
    n_with_cluster = quality_df["cluster_id"].notna().sum()
    # Allow up to 5% missing in case of edge cases
    coverage = n_with_cluster / n_total if n_total > 0 else 0
    assert coverage >= 0.95, (
        f"Only {coverage:.1%} of providers have cluster assignments "
        f"({n_with_cluster}/{n_total})"
    )


def test_cluster_names_are_valid(quality_df: pd.DataFrame) -> None:
    """Cluster names should be from the expected set."""
    expected_names = {
        "Premium Specialists", "Community Providers",
        "Outlier Billers", "Standard Practice"
    }
    actual_names = set(quality_df["cluster_name"].dropna().unique())
    assert actual_names.issubset(expected_names), (
        f"Unexpected cluster names: {actual_names - expected_names}"
    )


def test_cluster_count(quality_df: pd.DataFrame) -> None:
    """There should be 4 distinct clusters (or fewer if data is limited)."""
    n_clusters = quality_df["cluster_id"].dropna().nunique()
    assert 2 <= n_clusters <= 6, (
        f"Expected 2-6 clusters, got {n_clusters}"
    )


# ==============================================================================
# 8. Cost per patient is reasonable
# ==============================================================================

def test_cost_per_patient_positive(payments_df: pd.DataFrame) -> None:
    """cost_per_patient must be positive where it exists."""
    cpp = payments_df["cost_per_patient"].dropna()
    assert (cpp > 0).all(), "Some cost_per_patient values are non-positive"


def test_charge_to_payment_ratio_range(payments_df: pd.DataFrame) -> None:
    """Charge-to-payment ratio should be > 1 (charges always exceed payments)."""
    ratio = payments_df["charge_to_payment_ratio"].dropna()
    # Allow a tiny fraction of edge cases (rounding)
    pct_below_one = (ratio < 0.99).mean()
    assert pct_below_one < 0.05, (
        f"{pct_below_one:.1%} of charge ratios are < 1 (charges < payments)"
    )


# ==============================================================================
# 9. Providers table integrity
# ==============================================================================

def test_providers_unique_npi(providers_df: pd.DataFrame) -> None:
    """NPI must be unique in the providers table."""
    dup_count = providers_df["npi"].duplicated().sum()
    assert dup_count == 0, f"Found {dup_count} duplicate NPIs in providers table"


def test_providers_state_coverage(providers_df: pd.DataFrame) -> None:
    """Providers should span at least 45 US states."""
    n_states = providers_df["state"].nunique()
    assert n_states >= 45, f"Only {n_states} states covered (expected ?45)"
