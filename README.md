# Polymarket Prediction Market Analytics Pipeline

An end-to-end data engineering project that ingests live prediction market data from Polymarket, processes it with PySpark, models it through a dbt pipeline, and serves insights via an interactive dashboard.

## Live Demo
[View Live Dashboard](https://polymarket-pipeline-5lkg9ykvlhvtwgwg85bbxq.streamlit.app/)

---

## Architecture

```
Polymarket Gamma API (live data)
       │
       ▼
Python Ingestion Script
  • 500 active markets
  • Keyword-based category classification
  • Saved as Parquet
       │
       ▼
PySpark Processing
  • Type casting and cleaning
  • Derived columns: days_until_close, volume_to_liquidity_ratio, is_high_volume
  • Category aggregations
  • Output: Parquet files
       │
       ▼
DuckDB Data Warehouse
  • raw_markets
  • raw_category_summary
       │
       ▼
dbt Transformation Layers
  ├── Staging: stg_markets
  └── Marts:   fct_category_volume, fct_top_markets, fct_market_timing
       │
       ▼
Streamlit Dashboard (Plotly charts)
```

---

## Key Insights

- Politics dominates with $1.7B in total volume across 177 markets
- Sports is second at $700M across 165 markets
- Crypto markets have the highest volume-to-liquidity ratio at 45x
- 500 active markets tracked with real-time 24hr volume data

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, requests, pandas |
| Processing | PySpark |
| Storage | DuckDB, Parquet |
| Transformation | dbt (dbt-duckdb) |
| Testing & Docs | dbt tests, dbt docs |
| Visualization | Streamlit, Plotly |
| Version Control | Git, GitHub |

---

## Project Structure

```
polymarket-pipeline/
├── ingestion/
│   └── fetch_markets.py        # Fetches and categorizes markets from Polymarket API
├── processing/
│   └── spark_transform.py      # PySpark transformations and aggregations
├── polymarket_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_markets.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── fct_category_volume.sql
│   │       ├── fct_top_markets.sql
│   │       ├── fct_market_timing.sql
│   │       └── schema.yml
│   └── dbt_project.yml
├── dashboard/
│   └── app.py                  # Streamlit dashboard
├── data/
│   └── exports/                # CSV exports for deployment
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.12
- Java 17 (required for PySpark)

### 1. Clone the repo
```bash
git clone https://github.com/YannnnnaY/polymarket-pipeline.git
cd polymarket-pipeline
```

### 2. Set up environment
```bash
python3.12 -m venv venv
source venv/bin/activate
pip install pyspark requests pandas duckdb dbt-duckdb streamlit plotly
```

### 3. Fetch live data
```bash
python3 ingestion/fetch_markets.py
```

### 4. Run PySpark processing
```bash
python3 processing/spark_transform.py
```

### 5. Load into DuckDB and run dbt
```bash
python3 -c "
import pandas as pd, duckdb, os
conn = duckdb.connect('data/polymarket.duckdb')
for f in ['processed_markets', 'category_summary']:
    df = pd.read_parquet(f'data/{f}.parquet')
    conn.execute(f'CREATE OR REPLACE TABLE raw_{f} AS SELECT * FROM df')
conn.close()
"
cd polymarket_dbt
dbt run
dbt test
```

### 6. Launch dashboard
```bash
cd ..
streamlit run dashboard/app.py
```

---

## Data Quality Tests

14 dbt tests covering:
- `not_null` on all critical columns
- `unique` on market_id and category
- `accepted_values` on category field

```bash
cd polymarket_dbt
dbt test
```

---

## dbt Documentation

```bash
cd polymarket_dbt
dbt docs generate
dbt docs serve --port 8083
```

---

## Future Improvements
- Schedule daily refresh with Airflow to track probability movements over time
- Add price history per market to show probability trends
- Migrate to BigQuery for cloud scale
- Add resolution tracking — did the predicted outcome happen?
- Expand category classification using NLP/LLM instead of keyword matching
