from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import sys

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    "healthcare_pipeline",
    default_args=default_args,
    description="End to end healthcare revenue analytics pipeline",
    schedule_interval="@quarterly",
    start_date=days_ago(1),
    catchup=False,
    tags=["healthcare", "batch", "etl"],
)

def run_ingestion():
    import os
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/pramodarun26/healthcare-sa-key.json"
    result = subprocess.run(
        ["python3", "/home/pramodarun26/healthcare_pipeline/ingestion/ingest_cms_data.py"],
        capture_output=True, text=True, env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")

def run_bigquery_load():
    import os
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/pramodarun26/healthcare-sa-key.json"
    result = subprocess.run(
        ["python3", "/home/pramodarun26/healthcare_pipeline/warehouse/load_bigquery.py"],
        capture_output=True, text=True, env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"BigQuery load failed: {result.stderr}")

def run_ml_training():
    import os
    env = os.environ.copy()
    env["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/pramodarun26/healthcare-sa-key.json"
    result = subprocess.run(
        ["python3", "/home/pramodarun26/healthcare_pipeline/ml/train_model.py"],
        capture_output=True, text=True, env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"ML training failed: {result.stderr}")

task_ingest = PythonOperator(
    task_id="ingest_cms_data",
    python_callable=run_ingestion,
    dag=dag,
)

task_etl = BashOperator(
    task_id="run_pyspark_etl",
    bash_command=(
        "gcloud dataproc jobs submit pyspark "
        "gs://healthcare-pipeline-raw-data/scripts/etl_transform.py "
        "--cluster=healthcare-cluster "
        "--region=us-central1 "
        "--project=healthcare-pipeline-489402"
    ),
    dag=dag,
)

task_load_bq = PythonOperator(
    task_id="load_bigquery",
    python_callable=run_bigquery_load,
    dag=dag,
)

task_train = PythonOperator(
    task_id="train_ml_model",
    python_callable=run_ml_training,
    dag=dag,
)

task_ingest >> task_etl >> task_load_bq >> task_train
