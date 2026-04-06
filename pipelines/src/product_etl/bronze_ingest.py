import os
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

BASE_PATH = "/opt/airflow/data/product_etl"
BRONZE = os.path.join(BASE_PATH, "bronze")

os.makedirs(BRONZE, exist_ok=True)

def run_bronze_ingestion(execution_date=None):
    hook = MsSqlHook(mssql_conn_id="sqlserver")

    tables = ['DimProduct', 'DimProductSubcategory', 'DimProductCategory']

    for table in tables:
        df = hook.get_pandas_df(f"SELECT * FROM {table}")
        df.to_csv(os.path.join(BRONZE, f"{table}.csv"), index=False)

    print("Bronze ingestion completed")