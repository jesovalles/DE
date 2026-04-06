from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.weather_data.fetch_weather import fetch_weather_data
from src.weather_data.store_weather import store_weather_data

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["tu-equipo@empresa.com"],
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Fetch weather data from OpenWeather API and store in Postgres",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "etl", "api"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    store_task = PythonOperator(
        task_id="store_weather",
        python_callable=store_weather_data,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    fetch_task >> store_task