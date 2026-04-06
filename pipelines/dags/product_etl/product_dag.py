from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.product_etl.bronze_ingest import run_bronze_ingestion
from src.product_etl.silver_transform import run_silver_transform
from src.product_etl.gold_aggregate import run_gold_aggregate
from src.product_etl.load_gold import load_gold_to_target

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="product_etl_medallion_pipeline",
    description="Medallion pipeline for product data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["product", "medallion", "etl"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingestion,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    load = PythonOperator(
        task_id="load_gold",
        python_callable=load_gold_to_target,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    bronze >> silver >> gold >> load