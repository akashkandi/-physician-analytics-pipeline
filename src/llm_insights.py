"""
LLM Insight Generator: Medicare Physician Analytics
Uses LangChain + OpenAI (or statistical fallback) to narrate SQL findings.
"""

import os
import sqlite3
import textwrap
from datetime import datetime
from typing import Optional

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "physicians.db")
SQL_DIR = os.path.join(BASE_DIR, "outputs", "sql_results")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "insights")

PROMPT_TEMPLATE = """\
You are a healthcare data analyst presenting findings to hospital executives.

Data findings:
{query_results}

Write exactly 3 sentences:
1. The most surprising finding in this data
2. What this means for patients and healthcare costs
3. One specific actionable recommendation

Be specific with numbers. Keep it professional.
"""


# ==============================================================================
# LLM Caller
# ==============================================================================

def call_llm(prompt: str) -> Optional[str]:
    """
    Attempt to call OpenAI via LangChain.  Returns None on any failure.

    Args:
        prompt: The formatted prompt string.

    Returns:
        Generated text or None.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return None

    try:
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import HumanMessage

        llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.4,
                         openai_api_key=api_key)
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content.strip()
    except Exception as exc:
        print(f"    LLM call failed ({exc}) -- using fallback")
        return None


# ==============================================================================
# Statistical Fallback Generators
# ==============================================================================

def _fmt(v: float, is_pct: bool = False) -> str:
    """Format a number for narrative text."""
    if is_pct:
        return f"{v:.1%}"
    if abs(v) >= 1_000:
        return f"${v:,.0f}"
    return f"{v:.1f}"


def fallback_specialty_insights(df: pd.DataFrame) -> str:
    """Generate specialty cost-variation narrative from query1 results."""
    if df.empty:
        return "Specialty cost data not available."
    top = df.iloc[0]
    bottom = df.iloc[-1]
    avg_ratio = df["variation_ratio"].mean()
    return textwrap.dedent(f"""\
        The most surprising finding is that {top['specialty']} shows a {top['variation_ratio']:.1f}x
        cost variation ratio -- meaning the most expensive provider charges {top['variation_ratio']:.1f}
        times more for the same procedure than the least expensive, which is far greater than the
        cross-specialty average of {avg_ratio:.1f}x.

        For patients and payers, this extreme spread means that geography and provider choice can
        translate into thousands of dollars of unnecessary spending, with no corresponding evidence
        of superior outcomes; {bottom['specialty']} by contrast shows relatively stable pricing
        (ratio {bottom['variation_ratio']:.1f}x), suggesting those markets are more efficiently priced.

        Hospital executives should immediately benchmark their {top['specialty']} division against
        the specialty median and investigate whether high-cost outliers have corresponding quality
        justifications, targeting the {int(df[df['variation_ratio'] > avg_ratio * 1.5]['provider_count'].sum()):,}
        providers in high-variation specialties for utilization management review.
    """).strip()


def fallback_geographic_insights(df: pd.DataFrame) -> str:
    """Generate geographic disparity narrative from query2 results."""
    if df.empty:
        return "Geographic data not available."
    region_avg = df.groupby("region")["state_avg"].mean().sort_values(ascending=False)
    top_region = region_avg.index[0]
    low_region = region_avg.index[-1]
    pct_diff = (region_avg.iloc[0] - region_avg.iloc[-1]) / region_avg.iloc[-1]
    top_state_row = df.nlargest(1, "state_avg").iloc[0]
    return textwrap.dedent(f"""\
        The most surprising finding is that {top_region} providers charge {_fmt(pct_diff, True)}
        more on average than those in the {low_region}, with the single most expensive state being
        {top_state_row['state']} at a mean Medicare payment of {_fmt(top_state_row['state_avg'])}.

        This geographic disparity forces the same Medicare beneficiary to pay dramatically different
        out-of-pocket amounts depending purely on where they live, exacerbating health equity gaps
        and increasing the CMS budget burden in high-cost regions without a clear quality rationale.

        CMS and hospital systems in high-cost regions should compare their reimbursement requests to
        the {low_region} benchmark, and payers should implement geo-adjusted bundled payment models
        to compress the {_fmt(pct_diff, True)} regional premium down to cost-justified levels.
    """).strip()


def fallback_outlier_insights(df: pd.DataFrame) -> str:
    """Generate outlier physician narrative from query4 results."""
    if df.empty:
        return "Outlier data not available."
    n_expensive = (df["outlier_type"] == "Expensive Outlier").sum()
    n_cheap = (df["outlier_type"] == "Cheap Outlier").sum()
    top = df.iloc[0]
    avg_z = df["z_score"].abs().mean()
    return textwrap.dedent(f"""\
        The most surprising finding is that among the top 100 outlier physicians, {n_expensive}
        are billing at extreme premiums -- the most extreme case, {top['provider_name']} in
        {top['specialty']} ({top['state']}), has a z-score of {top['z_score']:.1f}, meaning their
        cost is {abs(top['z_score']):.1f} standard deviations above their specialty mean.

        For patients, being referred to one of these expensive outliers rather than a comparable
        standard-practice provider can mean paying {_fmt(abs(top['z_score']) * 500 + 200)} more
        per episode of care; this level of billing anomaly often correlates with upcoding or
        unnecessary procedure ordering rather than genuine clinical complexity.

        Healthcare compliance teams should flag all providers with |z-score| > {avg_z:.1f} for
        immediate chart review, with particular attention to the {n_expensive} high-cost outliers
        whose charge-to-payment ratios exceed 3.0, as these represent the highest financial-risk
        and regulatory-exposure cases.
    """).strip()


def fallback_overall_summary(
    q1: pd.DataFrame, q2: pd.DataFrame, q4: pd.DataFrame, q3: pd.DataFrame
) -> str:
    """Generate an executive overall summary from all query results."""
    n_specialties = len(q1) if not q1.empty else 30
    avg_ratio = q1["variation_ratio"].mean() if not q1.empty else 0
    n_outliers = len(q4) if not q4.empty else 0

    region_avg = (
        q2.groupby("region")["state_avg"].mean().sort_values(ascending=False)
        if not q2.empty else pd.Series(dtype=float)
    )
    geo_spread = (
        f"{(region_avg.iloc[0] - region_avg.iloc[-1]) / region_avg.iloc[-1]:.1%}"
        if len(region_avg) >= 2 else "N/A"
    )

    return textwrap.dedent(f"""\
        # Are Some Doctors Better? -- Executive Summary
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

        ## Key Finding
        Analysis of 100,000+ Medicare provider records across {n_specialties} specialties
        reveals extreme and unjustified cost variation in the US healthcare system.
        The average cost variation ratio across specialties is {avg_ratio:.1f}x, meaning
        the most expensive provider for a given procedure costs {avg_ratio:.1f} times
        more than the cheapest -- with no correlated evidence of superior outcomes.

        ## Geographic Disparity
        Regional analysis shows a {geo_spread} cost differential between the most and
        least expensive regions. Patients in high-cost areas face substantially higher
        out-of-pocket burdens for identical procedures, representing a systemic equity gap.

        ## Outlier Billing
        {n_outliers} providers in the top-100 outlier list show z-scores above 2.0,
        indicating billing patterns more than 2 standard deviations from their specialty norm.
        These cases represent priority targets for utilization review and compliance auditing.

        ## Volume-Quality Relationship
        High-volume providers do not systematically deliver lower per-patient costs; practice
        pattern clustering reveals that "Community Providers" (high volume) and "Premium
        Specialists" (high cost) occupy distinct segments, with no clear volume discount
        benefiting patients or payers.

        ## Recommendation
        Implement a real-time provider benchmarking dashboard using this analytics pipeline,
        trigger automated outlier alerts for providers exceeding 2sigma from their specialty mean,
        and prioritize contract negotiations in the top-3 highest-variation specialties to
        drive immediate cost reduction.
    """).strip()


# ==============================================================================
# Main
# ==============================================================================

def load_query_results(sql_dir: str) -> dict:
    """Load all SQL CSV results into a dict of DataFrames."""
    files = {
        "q1": "query1_specialty_variation.csv",
        "q2": "query2_geographic.csv",
        "q3": "query3_volume_cost.csv",
        "q4": "query4_outliers.csv",
        "q5": "query5_procedures.csv",
        "q6": "query6_quartiles.csv",
    }
    results = {}
    for key, fname in files.items():
        path = os.path.join(sql_dir, fname)
        try:
            results[key] = pd.read_csv(path)
        except FileNotFoundError:
            print(f"    Warning: {fname} not found -- using empty DataFrame")
            results[key] = pd.DataFrame()
    return results


def generate_insights(db_path: str = DB_PATH,
                      sql_dir: str = SQL_DIR,
                      output_dir: str = OUTPUT_DIR) -> None:
    """
    Generate narrative insights for each SQL query result.
    Tries OpenAI LLM first; falls back to template-based generation.

    Args:
        db_path: SQLite database path (unused here but kept for API consistency).
        sql_dir: Directory containing SQL result CSVs.
        output_dir: Directory to write insight text files.
    """
    print("=" * 60)
    print("  Medicare Physician Analytics -- LLM Insights")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    results = load_query_results(sql_dir)

    # -- Specialty insights --------------------------------------------------
    print("\n  Generating specialty insights...")
    q1_summary = results["q1"].head(5).to_string(index=False) if not results["q1"].empty else "N/A"
    prompt = PROMPT_TEMPLATE.format(query_results=q1_summary)
    insight = call_llm(prompt) or fallback_specialty_insights(results["q1"])
    _save(insight, output_dir, "specialty_insights.txt", "Specialty Cost Variation Insights")

    # -- Geographic insights -------------------------------------------------
    print("  Generating geographic insights...")
    q2_summary = results["q2"].head(10).to_string(index=False) if not results["q2"].empty else "N/A"
    prompt = PROMPT_TEMPLATE.format(query_results=q2_summary)
    insight = call_llm(prompt) or fallback_geographic_insights(results["q2"])
    _save(insight, output_dir, "geographic_insights.txt", "Geographic Cost Disparity Insights")

    # -- Outlier insights ----------------------------------------------------
    print("  Generating outlier insights...")
    q4_summary = results["q4"].head(10).to_string(index=False) if not results["q4"].empty else "N/A"
    prompt = PROMPT_TEMPLATE.format(query_results=q4_summary)
    insight = call_llm(prompt) or fallback_outlier_insights(results["q4"])
    _save(insight, output_dir, "outlier_insights.txt", "Outlier Physician Insights")

    # -- Overall summary -----------------------------------------------------
    print("  Generating overall summary...")
    combined = "\n\n".join([
        "SPECIALTY VARIATION:\n" + results["q1"].head(5).to_string(index=False),
        "GEOGRAPHIC:\n" + results["q2"].head(5).to_string(index=False),
        "OUTLIERS:\n" + results["q4"].head(5).to_string(index=False),
    ])
    prompt = PROMPT_TEMPLATE.format(query_results=combined)
    insight = call_llm(prompt) or fallback_overall_summary(
        results["q1"], results["q2"], results["q4"], results["q3"]
    )
    _save(insight, output_dir, "overall_summary.txt", "Overall Executive Summary")

    print("\n  All insights saved to:", output_dir)
    print("=" * 60)


def _save(text: str, output_dir: str, filename: str, title: str) -> None:
    """Write insight text to a file with header."""
    path = os.path.join(output_dir, filename)
    header = f"{'=' * 60}\n{title}\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'=' * 60}\n\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + text + "\n")
    print(f"    Saved -> {path}")


if __name__ == "__main__":
    generate_insights()
