"""
FastAPI Service: Medicare Physician Analytics
Exposes provider profiles, specialty analytics, outlier lists, and dashboard data.

Run with:
    uvicorn src.api:app --reload --port 8000
"""

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
INSIGHTS_DIR = os.path.join(BASE_DIR, "outputs", "insights")

app = FastAPI(
    title="Physician Analytics API",
    description="Medicare provider cost analysis and benchmarking",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -- DB helper ------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    """Open a SQLite connection with row_factory for dict-style access."""
    if not os.path.exists(DB_PATH):
        raise HTTPException(
            status_code=503,
            detail=f"Database not found at {DB_PATH}. Run data_pipeline.py first.",
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def read_insight(filename: str) -> str:
    """Read an insight text file, return empty string on failure."""
    path = os.path.join(INSIGHTS_DIR, filename)
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "Insight not yet generated. Run llm_insights.py first."


# ==============================================================================
# GET /health
# ==============================================================================

@app.get("/health", summary="Pipeline health check")
def health() -> Dict[str, Any]:
    """
    Returns pipeline status, database record counts, and last update time.
    """
    try:
        conn = get_conn()
        tables = {}
        for table in ["providers", "procedures", "payments", "quality_metrics"]:
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            tables[table] = row["n"]
        conn.close()
        db_mtime = datetime.fromtimestamp(os.path.getmtime(DB_PATH)).isoformat()
        return {
            "status": "healthy",
            "database": DB_PATH,
            "last_updated": db_mtime,
            "record_counts": tables,
            "api_version": "1.0.0",
        }
    except Exception as exc:
        return {"status": "unhealthy", "error": str(exc)}


# ==============================================================================
# GET /provider/{npi}
# ==============================================================================

@app.get("/provider/{npi}", summary="Full provider profile")
def get_provider(npi: str) -> Dict[str, Any]:
    """
    Returns a complete profile for a single provider by NPI, including
    cluster assignment, cost benchmarks, and an LLM-generated summary.
    """
    conn = get_conn()

    provider = conn.execute(
        "SELECT * FROM providers WHERE npi = ?", (npi,)
    ).fetchone()
    if not provider:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Provider NPI {npi} not found")

    pay = conn.execute(
        """SELECT AVG(avg_medicare_payment) AS avg_cost,
                  AVG(cost_z_score)         AS avg_z_score,
                  AVG(charge_to_payment_ratio) AS avg_charge_ratio
           FROM payments WHERE npi = ?""",
        (npi,),
    ).fetchone()

    quality = conn.execute(
        "SELECT * FROM quality_metrics WHERE npi = ?", (npi,)
    ).fetchone()

    procs = conn.execute(
        "SELECT hcpcs_code, hcpcs_description, total_services FROM procedures WHERE npi = ?",
        (npi,),
    ).fetchall()

    conn.close()

    avg_cost = round(pay["avg_cost"] or 0, 2)
    avg_z = round(pay["avg_z_score"] or 0, 3)
    cluster = quality["cluster_name"] if quality and quality["cluster_name"] else "Unassigned"
    perf_tier = quality["performance_tier"] if quality and quality["performance_tier"] else "Unknown"

    # Simple LLM summary (template-based)
    llm_summary = (
        f"{dict(provider)['provider_name']} is a "
        f"{dict(provider)['specialty']} specialist based in "
        f"{dict(provider)['city']}, {dict(provider)['state']}. "
        f"Their average Medicare payment of ${avg_cost:,.2f} places them "
        f"{'above' if avg_z > 0 else 'below'} the specialty mean by "
        f"{abs(avg_z):.2f} standard deviations (z-score: {avg_z:.2f}). "
        f"Classified as '{cluster}', they are ranked '{perf_tier}' within their peer group."
    )

    return {
        "npi": npi,
        "name": dict(provider)["provider_name"],
        "specialty": dict(provider)["specialty"],
        "state": dict(provider)["state"],
        "city": dict(provider)["city"],
        "region": dict(provider)["region"],
        "cluster": cluster,
        "avg_cost": avg_cost,
        "cost_z_score": avg_z,
        "charge_to_payment_ratio": round(pay["avg_charge_ratio"] or 0, 3),
        "performance_tier": perf_tier,
        "total_patients": dict(provider)["total_patients"],
        "volume_tier": dict(provider)["volume_tier"],
        "procedure_diversity": quality["procedure_diversity"] if quality else 0,
        "procedures": [dict(p) for p in procs],
        "llm_summary": llm_summary,
    }


# ==============================================================================
# GET /specialty/{specialty}
# ==============================================================================

@app.get("/specialty/{specialty}", summary="Specialty analytics")
def get_specialty(specialty: str) -> Dict[str, Any]:
    """
    Returns aggregate analytics for a medical specialty including cost stats,
    variation ratio, top states, and outlier count.
    """
    conn = get_conn()

    row = conn.execute(
        """SELECT COUNT(DISTINCT p.npi)                   AS provider_count,
                  AVG(pay.avg_medicare_payment)            AS avg_cost,
                  MIN(pay.avg_medicare_payment)            AS min_cost,
                  MAX(pay.avg_medicare_payment)            AS max_cost,
                  SUM(p.total_patients)                    AS total_patients
           FROM providers p
           JOIN payments pay ON p.npi = pay.npi
           WHERE p.specialty = ?""",
        (specialty,),
    ).fetchone()

    if not row or row["provider_count"] == 0:
        conn.close()
        raise HTTPException(
            status_code=404,
            detail=f"Specialty '{specialty}' not found or has no data",
        )

    outlier_count = conn.execute(
        """SELECT COUNT(*) AS n
           FROM providers p JOIN payments pay ON p.npi = pay.npi
           WHERE p.specialty = ? AND pay.is_outlier = 1""",
        (specialty,),
    ).fetchone()["n"]

    top_states = pd.read_sql(
        """SELECT p.state, COUNT(DISTINCT p.npi) AS cnt
           FROM providers p
           WHERE p.specialty = ?
           GROUP BY p.state ORDER BY cnt DESC LIMIT 3""",
        sqlite3.connect(DB_PATH),
        params=(specialty,),
    )["state"].tolist()

    conn.close()

    min_cost = row["min_cost"] or 0
    max_cost = row["max_cost"] or 0
    variation_ratio = round(max_cost / min_cost, 1) if min_cost > 0 else None

    return {
        "specialty": specialty,
        "provider_count": row["provider_count"],
        "avg_cost": round(row["avg_cost"] or 0, 2),
        "min_cost": round(min_cost, 2),
        "max_cost": round(max_cost, 2),
        "cost_variation_ratio": variation_ratio,
        "total_patients": int(row["total_patients"] or 0),
        "outlier_count": outlier_count,
        "top_states": top_states,
    }


# ==============================================================================
# GET /outliers
# ==============================================================================

@app.get("/outliers", summary="Top outlier physicians")
def get_outliers(
    specialty: Optional[str] = Query(None, description="Filter by specialty"),
    state: Optional[str] = Query(None, description="Filter by state code"),
    limit: int = Query(20, ge=1, le=200, description="Max results to return"),
) -> List[Dict[str, Any]]:
    """
    Returns the top outlier physicians sorted by absolute z-score.
    Optionally filter by specialty and/or state.
    """
    conn = get_conn()

    sql = """
        SELECT p.npi, p.provider_name, p.specialty, p.state, p.city,
               p.volume_tier,
               ROUND(pay.avg_medicare_payment, 2)    AS their_cost,
               ROUND(pay.cost_z_score, 3)             AS z_score,
               ROUND(pay.charge_to_payment_ratio, 3)  AS charge_ratio,
               CASE WHEN pay.cost_z_score > 2 THEN 'Expensive Outlier'
                    WHEN pay.cost_z_score < -2 THEN 'Cheap Outlier'
               END AS outlier_type,
               q.cluster_name,
               q.performance_tier
        FROM providers p
        JOIN payments pay ON p.npi = pay.npi
        LEFT JOIN quality_metrics q ON p.npi = q.npi
        WHERE pay.is_outlier = 1
    """
    params: list = []
    if specialty:
        sql += " AND p.specialty = ?"
        params.append(specialty)
    if state:
        sql += " AND p.state = ?"
        params.append(state.upper())

    sql += " ORDER BY ABS(pay.cost_z_score) DESC"
    sql += f" LIMIT {limit}"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==============================================================================
# GET /dashboard-data
# ==============================================================================

@app.get("/dashboard-data", summary="Aggregated data for Power BI dashboard")
def dashboard_data() -> Dict[str, Any]:
    """
    Returns all aggregated data needed to populate the Power BI dashboard
    across all 6 pages.
    """
    conn = sqlite3.connect(DB_PATH)

    # Summary KPIs
    kpis = {
        "total_providers": pd.read_sql(
            "SELECT COUNT(DISTINCT npi) AS n FROM providers", conn
        )["n"].iloc[0],
        "total_procedures": pd.read_sql(
            "SELECT COUNT(*) AS n FROM procedures", conn
        )["n"].iloc[0],
        "total_outliers": pd.read_sql(
            "SELECT COUNT(*) AS n FROM payments WHERE is_outlier=1", conn
        )["n"].iloc[0],
        "avg_variation_ratio": None,
    }

    # Specialty variation
    spec_var = pd.read_sql(
        """SELECT specialty,
                  COUNT(DISTINCT p.npi) AS provider_count,
                  ROUND(AVG(pay.avg_medicare_payment), 2) AS avg_cost,
                  ROUND(MAX(pay.avg_medicare_payment) * 1.0 /
                        NULLIF(MIN(pay.avg_medicare_payment), 0), 1) AS variation_ratio
           FROM providers p JOIN payments pay ON p.npi = pay.npi
           GROUP BY specialty HAVING provider_count > 10
           ORDER BY avg_cost DESC LIMIT 30""",
        conn,
    )
    if not spec_var.empty and "variation_ratio" in spec_var.columns:
        kpis["avg_variation_ratio"] = round(spec_var["variation_ratio"].mean(), 1)

    # Geographic
    geo = pd.read_sql(
        """SELECT p.state, p.region,
                  ROUND(AVG(pay.avg_medicare_payment), 2) AS avg_cost,
                  COUNT(DISTINCT p.npi) AS provider_count
           FROM providers p JOIN payments pay ON p.npi = pay.npi
           GROUP BY p.state, p.region
           ORDER BY avg_cost DESC""",
        conn,
    )

    # Cluster distribution
    clusters = pd.read_sql(
        """SELECT cluster_name,
                  COUNT(*) AS provider_count,
                  ROUND(AVG(avg_cost_z_score), 3) AS avg_z_score
           FROM quality_metrics WHERE cluster_name IS NOT NULL
           GROUP BY cluster_name""",
        conn,
    )

    # Top outliers for dashboard
    outliers = pd.read_sql(
        """SELECT p.provider_name, p.specialty, p.state,
                  ROUND(pay.avg_medicare_payment, 2) AS cost,
                  ROUND(pay.cost_z_score, 2) AS z_score,
                  CASE WHEN pay.cost_z_score > 2 THEN 'Expensive' ELSE 'Cheap' END AS type
           FROM providers p JOIN payments pay ON p.npi = pay.npi
           WHERE pay.is_outlier=1
           ORDER BY ABS(pay.cost_z_score) DESC LIMIT 50""",
        conn,
    )

    # Executive summary insight
    summary_insight = read_insight("overall_summary.txt")

    conn.close()

    return {
        "kpis": kpis,
        "specialty_variation": spec_var.to_dict(orient="records"),
        "geographic": geo.to_dict(orient="records"),
        "clusters": clusters.to_dict(orient="records"),
        "top_outliers": outliers.to_dict(orient="records"),
        "executive_summary": summary_insight[:2000],  # truncate for API
        "generated_at": datetime.now().isoformat(),
    }


# ==============================================================================
# GET /specialties  (helper)
# ==============================================================================

@app.get("/specialties", summary="List all available specialties")
def list_specialties() -> List[str]:
    """Returns a sorted list of all specialties in the database."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT specialty FROM providers ORDER BY specialty"
    ).fetchall()
    conn.close()
    return [r["specialty"] for r in rows]
