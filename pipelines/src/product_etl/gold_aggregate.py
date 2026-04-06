import os
import pandas as pd

BASE_PATH = "/opt/airflow/data/product_etl"
SILVER = os.path.join(BASE_PATH, "silver")
GOLD = os.path.join(BASE_PATH, "gold")

os.makedirs(GOLD, exist_ok=True)

def run_gold_aggregate(execution_date=None):

    p = pd.read_csv(os.path.join(SILVER, "DimProduct.csv"))
    ps = pd.read_csv(os.path.join(SILVER, "DimProductSubcategory.csv"))
    pc = pd.read_csv(os.path.join(SILVER, "DimProductCategory.csv"))

    p['ProductSubcategoryKey'] = p['ProductSubcategoryKey'].astype(float).astype(int)

    merged = p.merge(ps, on='ProductSubcategoryKey') \
              .merge(pc, on='ProductCategoryKey')

    merged.to_csv(os.path.join(GOLD, "DimProductCategory.csv"), index=False)

    print("Gold aggregation completed")