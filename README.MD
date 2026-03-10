# Healthcare Revenue Analytics Pipeline

An end-to-end batch data pipeline that ingests CMS Medicare data, processes it with PySpark, stores it in BigQuery, trains ML models tracked with MLflow, orchestrates everything with Airflow, and serves insights through a Flask dashboard deployed on Cloud Run.



## What This Project Does

Every quarter, this pipeline automatically:
1. Fetches 50,000+ Medicare records from the CMS public API
2. Stores raw data in Google Cloud Storage (acting as a data lake)
3. Cleans and transforms the data using PySpark on Dataproc
4. Loads it into a star schema data warehouse in BigQuery
5. Trains 3 ML models to predict Medicare payment amounts, tracked with MLflow
6. Serves the results through an interactive web dashboard

## Architecture
```
CMS Medicare API → GCS (Data Lake) → PySpark ETL on Dataproc → BigQuery (Star Schema)
                                                                        ↓
                                                            MLflow Model Training
                                                                        ↓
                                                        Flask Dashboard on Cloud Run
                                                                        ↑
                                                        Airflow DAG Orchestration
```

## Tech Stack

- **Ingestion:** Python, CMS Medicare public API
- **Storage:** Google Cloud Storage (data lake), BigQuery (data warehouse)
- **Processing:** PySpark on Google Dataproc
- **Orchestration:** Apache Airflow on Compute Engine
- **ML Tracking:** MLflow
- **Dashboard:** Flask, Chart.js, Docker, Cloud Run
- **Cloud:** GCP (GCS, Dataproc, BigQuery, Compute Engine, Cloud Run)

## Data

Source: CMS Medicare Physician & Other Practitioners by Provider and Service (2022)

- 50,000 records across 28 columns
- Covers provider type, location, procedure codes, and payment amounts

## Data Warehouse Schema

**fact_claims** (50,000 rows) — core metrics per provider-procedure combination

**dim_provider** (6,734 rows) — provider details including type, name, location

**dim_procedure** (1,992 rows) — HCPCS procedure codes and descriptions

## ML Results

Target variable: `avg_medicare_payment`

| Model | R2 Score | RMSE | MAE |
|-------|----------|------|-----|
| Linear Regression | 0.6486 | 112.74 | 40.00 |
| Random Forest | 0.6440 | 113.48 | 35.12 |
| Gradient Boosting | 0.4071 | 146.44 | 35.24 |

All experiments tracked and versioned in MLflow.

## Project Structure
```
healthcare-pipeline/
    ingestion/
        ingest_cms_data.py        # Fetches data from CMS API, uploads to GCS
    etl/
        etl_transform.py          # PySpark ETL — cleaning, transformation, partitioning
        explore.py                # EDA script
    warehouse/
        load_bigquery.py          # Loads curated Parquet files into BigQuery
    ml/
        train_model.py            # Trains 3 models, logs results to MLflow
    dags/
        healthcare_dag.py         # Airflow DAG — orchestrates all 4 pipeline steps
    flask_app/
        app.py                    # Flask API with BigQuery endpoints
        templates/index.html      # Dashboard UI with Chart.js charts
        Dockerfile                # Container config for Cloud Run
        requirements.txt
```

## Pipeline DAG

The Airflow DAG runs quarterly with 4 tasks in sequence:
```
ingest_cms_data → run_pyspark_etl → load_bigquery → train_ml_model
```

## Key Insights from the Data

- **California** leads all states with $85M+ in total Medicare payments
- **Internal Medicine** is the top provider type at $65M+
- **Nurse Practitioners** have the most providers (1,320) in the dataset
- Linear Regression achieves the best R2 score (0.65) for payment prediction

## Setup

### Prerequisites
- GCP account with billing enabled
- Python 3.10+
- Google Cloud SDK

### GCP Services Used
- Cloud Storage — raw data lake
- Dataproc — PySpark processing
- BigQuery — data warehouse
- Compute Engine — Airflow + MLflow server
- Cloud Run — Flask dashboard

### Environment Variables
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
MLFLOW_TRACKING_URI=http://35.223.249.176:5000/
```

## What I Learned

Building this project gave me hands-on experience with the full data engineering lifecycle, from raw API ingestion through to a live deployed dashboard. The most valuable lessons were understanding how GCS mirrors HDFS concepts in a cloud-native way, designing star schemas for analytical workloads, and managing distributed Spark jobs on Dataproc. Integrating MLflow into a batch pipeline showed how experiment tracking fits naturally into the DE workflow rather than being a separate ML concern.
