import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.flight_ops.bronze_ingest import run_bronze_ingestion
from src.flight_ops.silver_transform import run_silver_transform
from src.flight_ops.gold_aggregate import run_gold_aggregate
from src.flight_ops.load_gold_to_snowflake import load_gold_to_snowflake

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["tu-equipo@empresa.com"],
}

with DAG(
    dag_id="flights_ops_medallion_pipeline",
    description="Medallion architecture pipeline for flight operations data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["flights", "medallion", "etl"],
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

    load_to_snowflake = PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    bronze >> silver >> gold >> load_to_snowflake