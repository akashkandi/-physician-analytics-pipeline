"""
SQL Analysis: Medicare Physician Analytics
Runs 6 analytical queries against the SQLite database and saves results.
"""

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "sql_results")


# -- Query definitions ----------------------------------------------------------

QUERIES = {
    "query1_specialty_variation": {
        "title": "Cost Variation by Specialty",
        "sql": """
            SELECT specialty,
                   COUNT(DISTINCT p.npi)                                AS provider_count,
                   ROUND(AVG(pay.avg_medicare_payment), 2)              AS avg_cost,
                   ROUND(MAX(pay.avg_medicare_payment), 2)              AS max_cost,
                   ROUND(MIN(pay.avg_medicare_payment), 2)              AS min_cost,
                   ROUND(MAX(pay.avg_medicare_payment) * 1.0 /
                         NULLIF(MIN(pay.avg_medicare_payment), 0), 1)   AS variation_ratio,
                   ROUND(AVG(pay.cost_z_score), 3)                      AS avg_z_score
            FROM providers p
            JOIN payments pay ON p.npi = pay.npi
            GROUP BY specialty
            HAVING provider_count > 20
            ORDER BY variation_ratio DESC
        """,
    },
    "query2_geographic": {
        "title": "Geographic Cost Variation",
        "sql": """
            SELECT p.state,
                   p.region,
                   p.specialty,
                   ROUND(AVG(pay.avg_medicare_payment), 2) AS state_avg,
                   COUNT(DISTINCT p.npi)                   AS provider_count
            FROM providers p
            JOIN payments pay ON p.npi = pay.npi
            GROUP BY p.state, p.region, p.specialty
            ORDER BY state_avg DESC
        """,
    },
    "query3_volume_cost": {
        "title": "Volume vs Cost Analysis",
        "sql": """
            SELECT p.volume_tier,
                   p.specialty,
                   ROUND(AVG(pay.avg_medicare_payment), 2)  AS avg_cost,
                   ROUND(AVG(pay.cost_per_patient), 2)      AS avg_cost_per_patient,
                   COUNT(DISTINCT p.npi)                    AS provider_count,
                   SUM(p.total_patients)                    AS total_patients_served
            FROM providers p
            JOIN payments pay ON p.npi = pay.npi
            GROUP BY p.volume_tier, p.specialty
            ORDER BY p.specialty, avg_cost
        """,
    },
    "query4_outliers": {
        "title": "Outlier Physicians",
        "sql": """
            SELECT p.provider_name,
                   p.specialty,
                   p.state,
                   p.city,
                   ROUND(pay.avg_medicare_payment, 2)      AS their_cost,
                   ROUND(pay.cost_z_score, 2)              AS z_score,
                   ROUND(pay.charge_to_payment_ratio, 2)   AS charge_ratio,
                   p.volume_tier,
                   CASE WHEN pay.cost_z_score > 2  THEN 'Expensive Outlier'
                        WHEN pay.cost_z_score < -2 THEN 'Cheap Outlier'
                   END AS outlier_type
            FROM providers p
            JOIN payments pay ON p.npi = pay.npi
            WHERE pay.is_outlier = 1
            ORDER BY ABS(pay.cost_z_score) DESC
            LIMIT 100
        """,
    },
    "query5_procedures": {
        "title": "Procedure Cost Comparison",
        "sql": """
            SELECT proc.hcpcs_description,
                   COUNT(DISTINCT p.npi)                                      AS providers_performing,
                   ROUND(AVG(pay.avg_medicare_payment), 2)                    AS avg_cost,
                   ROUND(MIN(pay.avg_medicare_payment), 2)                    AS min_cost,
                   ROUND(MAX(pay.avg_medicare_payment), 2)                    AS max_cost,
                   ROUND(MAX(pay.avg_medicare_payment) -
                         MIN(pay.avg_medicare_payment), 2)                    AS cost_spread
            FROM procedures proc
            JOIN payments pay ON proc.npi = pay.npi AND proc.hcpcs_code = pay.hcpcs_code
            JOIN providers p ON proc.npi = p.npi
            GROUP BY proc.hcpcs_description
            HAVING providers_performing > 10
            ORDER BY cost_spread DESC
            LIMIT 20
        """,
    },
    "query6_quartiles": {
        "title": "Top vs Bottom Performers Same Procedure",
        "sql": """
            SELECT proc.hcpcs_description,
                   p.provider_name,
                   p.specialty,
                   p.state,
                   ROUND(pay.avg_medicare_payment, 2) AS cost,
                   p.total_patients,
                   NTILE(4) OVER (
                       PARTITION BY proc.hcpcs_code
                       ORDER BY pay.avg_medicare_payment
                   ) AS cost_quartile
            FROM procedures proc
            JOIN payments pay ON proc.npi = pay.npi
            JOIN providers p ON proc.npi = p.npi
            ORDER BY proc.hcpcs_description, cost
        """,
    },
}


def run_query(conn: sqlite3.Connection, name: str, meta: dict) -> pd.DataFrame:
    """
    Execute a single SQL query and return results as a DataFrame.

    Args:
        conn: Open SQLite connection.
        name: Query key name.
        meta: Dict containing 'title' and 'sql'.

    Returns:
        Query result as DataFrame.
    """
    print(f"\n  Running: {meta['title']}")
    try:
        df = pd.read_sql_query(meta["sql"], conn)
        print(f"    -> {len(df):,} rows returned")
        return df
    except Exception as exc:
        print(f"    FAIL Query failed: {exc}")
        return pd.DataFrame()


def print_summary(name: str, df: pd.DataFrame) -> None:
    """Print a human-readable preview of query results."""
    if df.empty:
        print("    (empty result)")
        return
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 120)
    print(df.head(10).to_string(index=False))


def run_all_queries(db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR) -> dict:
    """
    Execute all 6 SQL queries, save CSVs, and print summaries.

    Args:
        db_path: Path to the SQLite database.
        output_dir: Directory to write CSV results.

    Returns:
        Dict mapping query name -> DataFrame.
    """
    print("=" * 60)
    print("  Medicare Physician Analytics -- SQL Analysis")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    results = {}

    for name, meta in QUERIES.items():
        df = run_query(conn, name, meta)
        results[name] = df

        csv_path = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"    Saved -> {csv_path}")
        print_summary(name, df)

    conn.close()

    # -- Summary stats ------------------------------------------------------
    print("\n" + "=" * 60)
    print("  SQL Analysis Summary")
    print("=" * 60)
    q1 = results.get("query1_specialty_variation", pd.DataFrame())
    if not q1.empty:
        top = q1.iloc[0]
        print(f"  Highest variation specialty: {top['specialty']} "
              f"(ratio {top['variation_ratio']:.1f}x)")
        avg_var = q1["variation_ratio"].mean()
        print(f"  Mean variation ratio across specialties: {avg_var:.1f}x")

    q4 = results.get("query4_outliers", pd.DataFrame())
    if not q4.empty:
        expensive = (q4["outlier_type"] == "Expensive Outlier").sum()
        cheap = (q4["outlier_type"] == "Cheap Outlier").sum()
        print(f"  Outlier physicians (top 100): {expensive} expensive, {cheap} cheap")

    q5 = results.get("query5_procedures", pd.DataFrame())
    if not q5.empty:
        top_proc = q5.iloc[0]
        print(f"  Widest procedure cost spread: {top_proc['hcpcs_description']} "
              f"(${top_proc['cost_spread']:,.2f})")

    print("\n  All query CSVs saved to:", output_dir)
    return results


if __name__ == "__main__":
    run_all_queries()
