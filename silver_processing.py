# Databricks notebook: silver_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, count, sum, when, isnan
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# Read raw CSV from Bronze
bronze_path = "abfss://bronze@<your-storage-account-name>.dfs.core.windows.net/random_data.csv"
df = spark.read.option("header", "true").csv(bronze_path)

# Log raw data metrics
raw_count = df.count()
print(f"Bronze layer record count: {raw_count}")

# Data quality checks
def log_data_quality(df, stage):
    """Log data quality metrics for a dataframe"""
    # Count nulls in each column
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(
            col(column).isNull() | 
            (col(column) == "") | 
            (col(column).isnan() if df.schema[column].dataType.simpleString() in ["double", "float"] else False)
        ).count()
        null_counts[column] = null_count
    
    # Log the metrics
    print(f"\n--- {stage} Data Quality Metrics ---")
    print(f"Total records: {df.count()}")
    print(f"Null counts by column: {null_counts}")
    print(f"Duplicate UniqueID count: {df.count() - df.dropDuplicates(['UniqueID']).count()}")
    
    return null_counts

# Log data quality before cleaning
log_data_quality(df, "Before Cleaning")

# Schema validation - ensure all expected columns exist
expected_columns = ["UniqueID", "FirstName", "LastName", "AddressZipCity", 
                    "Birthday", "Price", "Stuckzahl", "SalesTax", "TaxAmount", 
                    "TotalPrice", "TotalAmount"]
missing_columns = [col for col in expected_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"Missing expected columns: {missing_columns}")

# Cleaning transformations
silver_df = df

# Apply transformations with better error handling
try:
    silver_df = (silver_df
        # Safely split AddressZipCity into Zip and City with error handling
        .withColumn("ZipCity_parts", split(col("AddressZipCity"), " "))
        .withColumn("Zip", col("ZipCity_parts").getItem(0))
        .withColumn("City", when(size(col("ZipCity_parts")) > 1, col("ZipCity_parts").getItem(1)).otherwise(None))
        .drop("ZipCity_parts")
        
        # Parse Birthday as date with error handling
        .withColumn("Birthday", to_date(col("Birthday"), "yyyy-MM-dd"))
        
        # Ensure numeric fields are typed correctly with validation
        .withColumn("Price", col("Price").cast("float"))
        .withColumn("Stuckzahl", col("Stuckzahl").cast("integer"))
        .withColumn("SalesTax", col("SalesTax").cast("float"))
        .withColumn("TaxAmount", col("TaxAmount").cast("float"))
        .withColumn("TotalPrice", col("TotalPrice").cast("float"))
        .withColumn("TotalAmount", col("TotalAmount").cast("float"))
        
        # Add data quality flag for validation
        .withColumn("HasQualityIssues", 
            (col("UniqueID").isNull()) | 
            (col("FirstName").isNull()) | 
            (col("LastName").isNull()) |
            (col("Birthday").isNull()) |
            (isnan("TotalAmount") | col("TotalAmount").isNull())
        )
    )
except Exception as e:
    error_message = f"Error during data transformation: {str(e)}"
    print(error_message)
    # Log the error appropriately or raise
    raise

# Log data quality after transformations but before filtering
log_data_quality(silver_df, "After Transformations")

# Filter out low-quality records but keep track of them
rejected_records = silver_df.filter(col("HasQualityIssues"))
rejected_count = rejected_records.count()

if rejected_count > 0:
    # Write rejected records to a separate location for investigation
    reject_path = "abfss://silver@<your-storage-account-name>.dfs.core.windows.net/rejected_records.parquet"
    rejected_records.write.mode("append").parquet(reject_path)
    print(f"Wrote {rejected_count} rejected records to {reject_path}")

# Finalize the silver dataset
silver_df = (silver_df
    .filter(~col("HasQualityIssues"))
    .drop("HasQualityIssues")
    # Remove duplicates based on UniqueID
    .dropDuplicates(["UniqueID"])
)

# Final data quality check after all cleaning
final_null_counts = log_data_quality(silver_df, "Final Clean Data")

# Calculate and log data quality score (simplified example)
columns_with_nulls = sum(1 for count in final_null_counts.values() if count > 0)
data_quality_score = 100 - (columns_with_nulls / len(final_null_counts) * 100)
print(f"Data Quality Score: {data_quality_score:.2f}%")

# Add processing metadata
silver_df = silver_df.withColumn("ProcessedTimestamp", lit(datetime.now().isoformat()))
silver_df = silver_df.withColumn("DataQualityScore", lit(data_quality_score))

# Write to Silver layer as Parquet
silver_path = "abfss://silver@<your-storage-account-name>.dfs.core.windows.net/cleaned_data.parquet"
silver_df.write.mode("overwrite").parquet(silver_path)

# Log final metrics
print(f"Bronze record count: {raw_count}")
print(f"Rejected record count: {rejected_count}")
print(f"Final silver record count: {silver_df.count()}")
print(f"Data cleaned and written to Silver layer: {silver_path}")
