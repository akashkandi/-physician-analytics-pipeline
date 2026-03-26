# Power BI Setup Guide — Medicare Physician Analytics Dashboard

## Overview
This guide connects Power BI Desktop to the 6 SQL result CSV files generated
by the pipeline to build a 6-page analytics dashboard.

---

## Step 1: Connect to Data Sources

1. Open **Power BI Desktop**
2. Click **Get Data → Text/CSV**
3. Import each file from `outputs/sql_results/`:

| File | Alias in Power BI |
|------|-------------------|
| `query1_specialty_variation.csv` | `SpecialtyVariation` |
| `query2_geographic.csv` | `Geographic` |
| `query3_volume_cost.csv` | `VolumeCost` |
| `query4_outliers.csv` | `Outliers` |
| `query5_procedures.csv` | `Procedures` |
| `query6_quartiles.csv` | `Quartiles` |

4. For each file: Click **Load** (or **Transform Data** to verify columns)
5. After loading all 6, go to **Model view** and verify relationships
   (no explicit FK relationships needed — all are flat CSV exports)

---

## Page 1 — National Overview

**Purpose:** Executive-level KPI summary of the entire dataset.

### Visuals:

**Card 1 — Total Providers**
- Field: `SpecialtyVariation[provider_count]` → SUM
- Title: "Total Providers Analyzed"
- Format: Comma-separated integer

**Card 2 — Avg Cost Variation Ratio**
- Field: `SpecialtyVariation[variation_ratio]` → AVERAGE
- Title: "Avg Cost Variation Ratio"
- Format: One decimal place + "x" suffix

**Card 3 — Total Outliers**
- Measure: `COUNTROWS(FILTER(Outliers, Outliers[z_score] <> BLANK()))`
- Title: "Outlier Physicians Detected"

**Bar Chart — Top 10 Specialties by Avg Cost**
- X-axis: `SpecialtyVariation[specialty]`
- Y-axis: `SpecialtyVariation[avg_cost]`
- Sort: Descending by avg_cost
- Top N filter: 10
- Format: Y-axis as currency ($)

**Map Visual (if Map available)**
- Location: `Geographic[state]`
- Values: `Geographic[state_avg]` (average)
- Color saturation: Low=blue, High=red

---

## Page 2 — Specialty Analysis

**Purpose:** Deep-dive into any single specialty's cost distribution.

### Visuals:

**Slicer — Select Specialty**
- Field: `SpecialtyVariation[specialty]`
- Type: Dropdown

**Bar Chart — Cost Distribution by Volume Tier**
- Data: `VolumeCost`
- X-axis: `VolumeCost[volume_tier]`
- Y-axis: `VolumeCost[avg_cost]`
- Legend: `VolumeCost[specialty]`
- Apply slicer filter

**Table — Provider Details**
- Columns: `provider_name`, `state`, `their_cost`, `z_score`, `charge_ratio`, `volume_tier`
- Source: `Outliers` (filtered by specialty slicer)
- Conditional formatting on `z_score`: red = high, green = low

**KPI Card — Variation Ratio vs National**
- Value: `SpecialtyVariation[variation_ratio]` for selected specialty
- Target: AVERAGE of all `variation_ratio` values
- Status indicator: Red if above avg, green if below

---

## Page 3 — Geographic View

**Purpose:** Understand how provider cost varies across states and regions.

### Visuals:

**Filled Map — States by Avg Cost**
- Location: `Geographic[state]`
- Color saturation: `Geographic[state_avg]`
- Tooltip: `provider_count`, `region`
- Color scale: Light yellow (low) → Dark red (high)

**Bar Chart — Region Comparison**
- X-axis: `Geographic[region]`
- Y-axis: `Geographic[state_avg]` → AVERAGE
- Sort: Descending
- Colors: Northeast=blue, Southeast=green, Midwest=orange, West=red

**Table — State Rankings**
- Columns: `state`, `region`, `specialty`, `state_avg`, `provider_count`
- Sort: `state_avg` descending
- Filterable by region slicer

**Slicer — Region Filter**
- Field: `Geographic[region]`

---

## Page 4 — Physician Clusters

**Purpose:** Visualize ML-derived practice pattern segments.

### Setup:
Load the cluster summary CSV:
**Get Data → Text/CSV → `outputs/clusters/cluster_summary.csv`**
Alias: `ClusterSummary`

### Visuals:

**Scatter Chart — Cost vs Volume by Cluster**
- X-axis: `VolumeCost[total_patients_served]`
- Y-axis: `VolumeCost[avg_cost]`
- Legend: `Outliers[cluster_name]` (or add cluster_name to VolumeCost)
- Note: May need to join via specialty lookup

**Donut Chart — Provider Distribution by Cluster**
- Legend: `ClusterSummary[cluster_name]`
- Values: `ClusterSummary[provider_count]` *(if this column exists)*
  Otherwise use COUNT of providers per cluster from the database export

**Table — Cluster Characteristics**
- Source: `ClusterSummary`
- Columns: `cluster_name`, `cluster_id`, all feature mean columns

**Bar Chart — Cluster by Region**
- Group cluster counts by region (requires joining Geographic + cluster data)

---

## Page 5 — Outlier Detection

**Purpose:** Identify and investigate anomalous billing patterns.

### Visuals:

**Table — All Outlier Physicians**
- Source: `Outliers`
- Columns: `provider_name`, `specialty`, `state`, `city`, `their_cost`,
  `z_score`, `charge_ratio`, `outlier_type`, `volume_tier`
- Filters: Specialty slicer, State slicer, Outlier type slicer
- Conditional formatting: `z_score` → red gradient for high values

**Bar Chart — Outliers by Specialty**
- X-axis: `Outliers[specialty]`
- Y-axis: COUNT of rows
- Legend: `Outliers[outlier_type]` (Expensive / Cheap)
- Colors: Red = expensive, blue = cheap

**Scatter — Z-Score Distribution**
- X-axis: `Outliers[their_cost]`
- Y-axis: `Outliers[z_score]`
- Color by: `Outliers[outlier_type]`
- Reference line at z=2 and z=-2

**Card Visuals:**
- "Total Outliers": COUNTROWS(Outliers)
- "% of Providers": Total outliers / total providers (DAX measure)
- "Avg Z-Score": AVERAGE(Outliers[z_score])

---

## Page 6 — Executive Summary

**Purpose:** Leadership-ready narrative summary of key findings.

### Visuals:

**Text Box — LLM-Generated Finding**
- Copy text from `outputs/insights/overall_summary.txt`
- Paste into a Power BI text box
- Format: 14pt font, dark background panel

**Table — Top 5 Key Findings**
Create a manual table using **Enter Data**:

| Finding | Metric | Insight |
|---------|--------|---------|
| Cost variation is extreme | ~15x avg ratio | Same procedure: 15× price difference |
| Northeast costs most | +20% above average | Geography drives cost more than quality |
| Outlier billers exist | ~5% of providers | Small group drives outsized cost |
| Volume ≠ lower cost | Cluster analysis | High-volume providers not cheaper |
| Specialty matters | Thoracic 18× spread | Surgical specialties most variable |

**Text Box — Methodology**
- ETL: pandas + SQLite
- Analysis: 6 SQL analytical queries
- Machine Learning: K-Means clustering (k=4), SHAP interpretation
- API: FastAPI REST service
- Automation: n8n workflow orchestration

---

## DAX Measures to Create

```dax
-- Total Unique Providers
Total Providers = DISTINCTCOUNT(SpecialtyVariation[specialty])

-- Outlier Rate
Outlier Rate =
    DIVIDE(
        COUNTROWS(Outliers),
        [Total Providers],
        0
    )

-- Avg Variation Ratio
Avg Variation = AVERAGE(SpecialtyVariation[variation_ratio])

-- High Cost Outliers
Expensive Outliers =
    COUNTROWS(
        FILTER(Outliers, Outliers[outlier_type] = "Expensive Outlier")
    )
```

---

## Publishing

1. Save the `.pbix` file to `outputs/physician_analytics_dashboard.pbix`
2. For Power BI Service: **File → Publish → My Workspace**
3. Schedule refresh: Set data gateway to auto-refresh from local CSVs weekly

---

## Refreshing Data

After re-running the pipeline:
1. Re-export CSVs are automatically placed in `outputs/sql_results/`
2. In Power BI Desktop: **Home → Refresh**
3. Or use the n8n workflow (Workflow 1) to trigger automatic refresh via API

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Map visual not showing | Enable Map visuals in File → Options → Security |
| CSV encoding error | Re-save CSVs as UTF-8 in pandas: `df.to_csv(..., encoding='utf-8-sig')` |
| Blank scatter chart | Check axis fields are numeric; use "Don't summarize" in field settings |
| Slicer not filtering | Verify cross-filter direction in Model view → Edit relationships |
