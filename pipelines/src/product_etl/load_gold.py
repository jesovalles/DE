import os
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

BASE_PATH = "/opt/airflow/data/product_etl"
GOLD = os.path.join(BASE_PATH, "gold")

def load_gold_to_target(execution_date=None):
    # conexión a Postgres desde Airflow
    conn = BaseHook.get_connection('postgres')

    engine = create_engine(
        f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    )

    # leer archivo gold
    df = pd.read_csv(os.path.join(GOLD, "DimProductCategory.csv"))

    # cargar a Postgres
    df.to_sql(
        name="prd_dim_product_category",
        con=engine,
        if_exists="append", 
        index=False
    )

    print("Data loaded successfully into Postgres")