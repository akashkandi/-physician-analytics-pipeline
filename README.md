# Are Some Doctors Better? — Medicare Physician Analytics Pipeline

> A complete end-to-end data analytics system for investigating cost variation
> among Medicare providers. Uses 100,000+ synthetic provider records to answer:
> **Do some doctors cost dramatically more for the same procedures?**

---

## Project Overview

This pipeline ingests Medicare Provider Utilization data, engineers features,
runs 6 SQL analytical queries, clusters providers with K-Means, interprets
clusters with SHAP, generates narrative insights, and exposes results via a
FastAPI REST service.

**Answer:** Yes — dramatically. The same procedure can vary by **15× or more**
in cost across providers, driven by specialty, geography, and billing patterns
rather than quality of care.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   DATA SOURCES                                  │
│  CMS Medicare CSV  OR  Synthetic Generator (100k+ rows)         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ETL PIPELINE  (data_pipeline.py)              │
│  Extract (chunked CSV) → Transform (features, z-scores)         │
│  Validate → Load (SQLite: 4 tables)                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼─────────────────┐
          ▼                ▼                 ▼
┌─────────────────┐ ┌───────────┐  ┌──────────────────┐
│ SQL ANALYSIS    │ │    EDA    │  │  ML CLUSTERING   │
│ sql_queries.py  │ │  eda.py   │  │ ml_clustering.py │
│ 6 queries →     │ │ 6 plots → │  │ K-Means (k=4)    │
│ CSV outputs     │ │ PNG files │  │ + SHAP → PNGs    │
└────────┬────────┘ └─────┬─────┘  └────────┬─────────┘
         │                │                  │
         └────────────────┼──────────────────┘
                          ▼
              ┌───────────────────────┐
              │  LLM INSIGHTS         │
              │  llm_insights.py      │
              │  OpenAI or fallback   │
              │  → .txt files         │
              └───────────┬───────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  FastAPI     │  │  Power BI    │  │  n8n         │
│  src/api.py  │  │  Dashboard   │  │  Workflows   │
│  :8000       │  │  6 pages     │  │  3 automations│
└──────────────┘  └──────────────┘  └──────────────┘
```

---

## Installation

### Prerequisites
- Python 3.10+
- pip

### Setup
```bash
# 1. Clone or extract the project
cd physician-analytics

# 2. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running the Pipeline

### Step 1 — Generate Data
```bash
python generate_data.py
# Creates: data/raw/medicare_providers.csv (100,000 rows)
```

### Step 2 — ETL Pipeline
```bash
python src/data_pipeline.py
# Creates: data/processed/physicians.db (4 tables)
```

### Step 3 — SQL Analysis
```bash
python src/sql_queries.py
# Creates: outputs/sql_results/*.csv (6 files)
```

### Step 4 — Exploratory Data Analysis
```bash
python src/eda.py
# Creates: outputs/eda/*.png (6 plots)
```

### Step 5 — ML Clustering
```bash
python src/ml_clustering.py
# Creates: outputs/clusters/*.png, cluster_summary.csv
# Updates: quality_metrics table in SQLite
```

### Step 6 — LLM Insights
```bash
# With OpenAI API key:
export OPENAI_API_KEY=sk-...
python src/llm_insights.py

# Without API key (uses statistical fallback):
python src/llm_insights.py
# Creates: outputs/insights/*.txt (4 files)
```

### Step 7 — Run Tests
```bash
pytest tests/test_pipeline.py -v
# Saves results summary: outputs/test_results.txt
```

### Step 8 — Start API Server
```bash
uvicorn src.api:app --reload --port 8000
# API docs: http://localhost:8000/docs
```

### Run Everything at Once
```bash
python generate_data.py && \
python src/data_pipeline.py && \
python src/sql_queries.py && \
python src/eda.py && \
python src/ml_clustering.py && \
python src/llm_insights.py && \
pytest tests/test_pipeline.py -v
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Pipeline status and DB counts |
| GET | `/provider/{npi}` | Full provider profile with cluster |
| GET | `/specialty/{specialty}` | Specialty aggregate analytics |
| GET | `/outliers` | Top outlier physicians (filterable) |
| GET | `/dashboard-data` | All data for Power BI in one call |
| GET | `/specialties` | List all available specialties |

**Example:**
```bash
curl http://localhost:8000/outliers?specialty=Cardiology&limit=10
```

---

## Key Findings

1. **Cost variation is extreme** — The same procedure varies by up to **15×**
   across providers in the same specialty. Thoracic Surgery shows the widest spread.

2. **Geography matters more than quality** — Northeast providers cost ~20% more
   than Midwest providers with no corresponding evidence of superior outcomes.

3. **Volume does not equal lower cost** — High-volume "Community Providers"
   are not systematically cheaper than "Premium Specialists." Volume and cost
   occupy independent dimensions in our cluster analysis.

4. **Outlier billers are specialty-specific** — ~5% of providers have cost
   z-scores above 2.0, concentrated in Oncology, Cardiology, and Surgery.

---

## Output Files

```
outputs/
├── eda/
│   ├── specialty_cost_distribution.png   # Box plot by specialty
│   ├── geographic_cost_variation.png     # State bar chart
│   ├── volume_vs_cost.png               # Scatter: patients vs cost
│   ├── outlier_distribution.png          # Z-score histogram
│   ├── variation_ratio.png              # Top-15 variation bar chart
│   └── regional_comparison.png          # Grouped region comparison
├── clusters/
│   ├── elbow_curve.png                  # K selection plot
│   ├── cluster_visualization.png        # PCA scatter
│   ├── shap_summary.png                 # SHAP feature importance
│   └── cluster_summary.csv             # Cluster characteristics
├── sql_results/
│   ├── query1_specialty_variation.csv
│   ├── query2_geographic.csv
│   ├── query3_volume_cost.csv
│   ├── query4_outliers.csv
│   ├── query5_procedures.csv
│   └── query6_quartiles.csv
├── insights/
│   ├── specialty_insights.txt
│   ├── geographic_insights.txt
│   ├── outlier_insights.txt
│   └── overall_summary.txt
├── final_report.md
├── powerbi_setup.md
└── n8n_workflows.md
```

---

## Power BI Dashboard Setup

See `outputs/powerbi_setup.md` for the full 6-page dashboard configuration guide.

**Quick start:**
1. Open Power BI Desktop
2. Get Data → Text/CSV → load all files from `outputs/sql_results/`
3. Build the 6 pages following the visual specifications in the setup guide

---

## n8n Automation Setup

See `outputs/n8n_workflows.md` for workflow JSON exports and setup instructions.

**Three workflows:**
- **Workflow 1**: Weekly data refresh (every Monday 8am)
- **Workflow 2**: Daily outlier alert (threshold-based email)
- **Workflow 3**: Provider lookup webhook (POST /lookup → JSON response)

---

## Configuration

| Variable | Location | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Environment | Optional — enables LLM insights |
| `DB_PATH` | `src/*.py` | SQLite database path |
| `RAW_DATA_PATH` | `src/data_pipeline.py` | Input CSV path |

---

## Testing

```bash
pytest tests/test_pipeline.py -v --tb=short

# Save results:
pytest tests/test_pipeline.py -v > outputs/test_results.txt 2>&1
```

**Tests cover:**
- Data loads with correct columns (100k+ rows)
- No negative payment values
- Z-scores calculated correctly
- Outlier flags match z-score criteria
- All 4 database tables exist and are non-empty
- All 6 SQL result CSVs present and non-empty
- Cluster assignments ≥ 95% coverage

---

## Methodology Notes

- **Data**: Synthetic Medicare data modeled on CMS 2022 provider utilization patterns
- **ETL**: pandas chunked loading, SQLite persistence, 4 normalized tables
- **Outlier definition**: |z-score| > 2 within specialty (captures ~5% of providers)
- **Clustering**: K-Means with k selected by silhouette score maximization
- **Interpretability**: SHAP TreeExplainer on Random Forest trained on cluster labels
- **Regions**: 5 US Census-aligned regions (Northeast, Southeast, Midwest, Southwest, West)
- **Reproducibility**: `random_state=42` used throughout

---

## Limitations

- Uses synthetic data — real CMS data may reveal different patterns
- Medicare only — private insurance excluded (often has different pricing dynamics)
- No direct quality outcome measures available in CMS utilization data
- 2022 data — healthcare costs evolve; patterns may shift post-pandemic
- Cost ≠ quality — this analysis cannot determine whether higher-cost providers
  deliver better patient outcomes

---

## Project Structure

```
physician-analytics/
├── data/
│   ├── raw/medicare_providers.csv        ← generated input
│   └── processed/physicians.db           ← SQLite database
├── src/
│   ├── data_pipeline.py                  ← ETL
│   ├── sql_queries.py                    ← 6 SQL queries
│   ├── eda.py                            ← 6 EDA plots
│   ├── ml_clustering.py                  ← K-Means + SHAP
│   ├── llm_insights.py                   ← LangChain insights
│   └── api.py                            ← FastAPI service
├── tests/
│   └── test_pipeline.py                  ← pytest suite
├── outputs/
│   ├── eda/                              ← PNG plots
│   ├── clusters/                         ← cluster plots + CSV
│   ├── sql_results/                      ← 6 query CSVs
│   ├── insights/                         ← insight .txt files
│   ├── final_report.md
│   ├── powerbi_setup.md
│   └── n8n_workflows.md
├── generate_data.py                      ← synthetic data generator
├── requirements.txt
└── README.md
```

---
