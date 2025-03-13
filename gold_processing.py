# Databricks notebook: gold_processing.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GoldProcessing").getOrCreate()

try:
    storage_account = dbutils.secrets.get(scope="medallion-kv", key="storage-account-name")
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/cleaned_data.parquet"
    df = spark.read.parquet(silver_path)
    
    # Optional: Add a simple transformation (e.g., filter valid purchases)
    gold_df = df.filter(col("TotalAmount") > 0)
    
    gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/purchases.parquet"
    gold_df.write.mode("overwrite").parquet(gold_path)
    
    print("Data written to Gold layer")
except Exception as e:
    print(f"Error processing data: {str(e)}")
    dbutils.notebook.exit(f"FAILED: {str(e)}")
