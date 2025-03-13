# Databricks notebook: gold_processing.py

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("GoldProcessing").getOrCreate()

# Read cleaned data from Silver
silver_path = "abfss://silver@<your-storage-account-name>.dfs.core.windows.net/cleaned_data.parquet"
df = spark.read.parquet(silver_path)

# For simplicity, Gold mirrors Silver (could add aggregations here if desired)
gold_df = df

# Write to Gold layer as Parquet
gold_path = "abfss://gold@<your-storage-account-name>.dfs.core.windows.net/purchases.parquet"
gold_df.write.mode("overwrite").parquet(gold_path)

print("Data written to Gold layer")
