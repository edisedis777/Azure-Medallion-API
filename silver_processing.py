# Databricks notebook: silver_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# Read raw CSV from Bronze
bronze_path = "abfss://bronze@<your-storage-account-name>.dfs.core.windows.net/random_data.csv"
df = spark.read.option("header", "true").csv(bronze_path)

# Cleaning transformations
silver_df = (df
    # Split AddressZipCity into Zip and City
    .withColumn("Zip", split(col("AddressZipCity"), " ")[0])
    .withColumn("City", split(col("AddressZipCity"), " ")[1])
    # Parse Birthday as date
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

# Write to Silver layer as Parquet
silver_path = "abfss://silver@<your-storage-account-name>.dfs.core.windows.net/cleaned_data.parquet"
silver_df.write.mode("overwrite").parquet(silver_path)

print("Data cleaned and written to Silver layer")
