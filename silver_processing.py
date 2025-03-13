# Databricks notebook: silver_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, regexp_extract, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import os

# Initialize Spark session
spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# Get storage account name from environment variable
storage_account = dbutils.secrets.get(scope="medallion-kv", key="storage-account-name")
if not storage_account:
    raise ValueError("Storage account name not found in secrets")

# Define schema for data validation
schema = StructType([
    StructField("UniqueID", StringType(), False),
    StructField("FirstName", StringType(), False),
    StructField("LastName", StringType(), False),
    StructField("AddressZipCity", StringType(), True),
    StructField("Birthday", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Stuckzahl", StringType(), True),
    StructField("SalesTax", StringType(), True),
    StructField("TaxAmount", StringType(), True),
    StructField("TotalPrice", StringType(), True),
    StructField("TotalAmount", StringType(), True),
    StructField("PurchaseType", StringType(), True)
])

# Log function for tracking metrics
def log_metrics(df, stage):
    record_count = df.count()
    null_counts = {col_name: df.filter(col(col_name).isNull()).count() 
                   for col_name in df.columns}
    
    print(f"=== {stage} Metrics ===")
    print(f"Record count: {record_count}")
    print("Null counts by column:")
    for col_name, count in null_counts.items():
        print(f"  {col_name}: {count}")
    return record_count

try:
    # Read raw CSV from Bronze with schema validation
    bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/random_data.csv"
    df = spark.read.option("header", "true").schema(schema).csv(bronze_path)
    
    # Log initial metrics
    bronze_count = log_metrics(df, "Bronze")
    
    # Cleaning transformations with improved error handling
    silver_df = (df
        # Split AddressZipCity into Zip and City with regex for better handling
        .withColumn("Zip", regexp_extract(col("AddressZipCity"), "^([0-9]+)", 1))
        .withColumn("City", regexp_extract(col("AddressZipCity"), "^[0-9]+\\s+(.+)$", 1))
        # Handle missing or unparseable cities
        .withColumn("City", when(col("City") == "", lit(None)).otherwise(col("City")))
        # Parse Birthday as date with error handling
        .withColumn("Birthday", to_date(col("Birthday"), "yyyy-MM-dd"))
        # Ensure numeric fields are typed correctly
        .withColumn("Price", col("Price").cast("float"))
        .withColumn("Stuckzahl", col("Stuckzahl").cast("integer"))
        .withColumn("SalesTax", col("SalesTax").cast("float"))
        .withColumn("TaxAmount", col("TaxAmount").cast("float"))
        .withColumn("TotalPrice", col("TotalPrice").cast("float"))
        .withColumn("TotalAmount", col("TotalAmount").cast("float"))
        # Remove duplicates based on UniqueID
        .dropDuplicates(["UniqueID"])
        # Drop rows with missing critical fields
        .na.drop(subset=["UniqueID", "FirstName", "LastName"])
    )
    
    # Log post-processing metrics
    silver_count = log_metrics(silver_df, "Silver")
    
    # Quality control check
    if silver_count < bronze_count * 0.9:
        raise ValueError(f"Data quality issue: Lost more than 10% of records during processing. " 
                         f"Bronze: {bronze_count}, Silver: {silver_count}")
    
    # Write to Silver layer as Parquet
    silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/cleaned_data.parquet"
    silver_df.write.mode("overwrite").parquet(silver_path)
    
    print("Data cleaned and written to Silver layer")
    
except Exception as e:
    print(f"Error processing data: {str(e)}")
    # Log the error to a central location or metrics system
    dbutils.notebook.exit(f"FAILED: {str(e)}")
