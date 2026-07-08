import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.bronze_ingest import run_bronze_ingestion
from scripts.silver_tranformer import run_silver_transform
from scripts.gold_aggregate import run_gold_aggregate
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="flight_ops_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 5),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
        
    )


    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
        
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
        
    )

    bronze >> silver >> gold