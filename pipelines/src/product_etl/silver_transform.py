import os
import pandas as pd

BASE_PATH = "/opt/airflow/data/product_etl"
BRONZE = os.path.join(BASE_PATH, "bronze")
SILVER = os.path.join(BASE_PATH, "silver")

os.makedirs(SILVER, exist_ok=True)

def run_silver_transform(execution_date=None):

    # -------- Product --------
    df = pd.read_csv(os.path.join(BRONZE, "DimProduct.csv"))

    revised = df[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey',
                  'WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName',
                  'StandardCost', 'FinishedGoodsFlag', 'Color', 'SafetyStockLevel',
                  'ReorderPoint', 'ListPrice', 'Size', 'SizeRange', 'Weight',
                  'DaysToManufacture', 'ProductLine', 'DealerPrice', 'Class',
                  'Style', 'ModelName', 'EnglishDescription', 'StartDate',
                  'EndDate', 'Status']]

    revised.fillna({
        'WeightUnitMeasureCode': '0',
        'ProductSubcategoryKey': '0',
        'SizeUnitMeasureCode': '0',
        'StandardCost': 0,
        'ListPrice': 0,
        'ProductLine': 'NA',
        'Class': 'NA',
        'Style': 'NA',
        'Size': 'NA',
        'ModelName': 'NA',
        'EnglishDescription': 'NA',
        'DealerPrice': 0,
        'Weight': 0
    }, inplace=True)

    revised.rename(columns={
        "EnglishDescription": "Description",
        "EnglishProductName": "ProductName"
    }, inplace=True)

    revised.to_csv(os.path.join(SILVER, "DimProduct.csv"), index=False)

    # -------- Subcategory --------
    df = pd.read_csv(os.path.join(BRONZE, "DimProductSubcategory.csv"))

    revised = df[['ProductSubcategoryKey',
                  'EnglishProductSubcategoryName',
                  'ProductSubcategoryAlternateKey',
                  'ProductCategoryKey']]

    revised.rename(columns={
        "EnglishProductSubcategoryName": "ProductSubcategoryName"
    }, inplace=True)

    revised.to_csv(os.path.join(SILVER, "DimProductSubcategory.csv"), index=False)

    # -------- Category --------
    df = pd.read_csv(os.path.join(BRONZE, "DimProductCategory.csv"))

    revised = df[['ProductCategoryKey',
                  'ProductCategoryAlternateKey',
                  'EnglishProductCategoryName']]

    revised.rename(columns={
        "EnglishProductCategoryName": "ProductCategoryName"
    }, inplace=True)

    revised.to_csv(os.path.join(SILVER, "DimProductCategory.csv"), index=False)

    print("Silver transformation completed")