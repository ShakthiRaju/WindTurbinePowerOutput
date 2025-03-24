# Databricks notebook source
# MAGIC %run /Users/shakthibiz16@gmail.com/WindTurbine/HelperFunctions

# COMMAND ----------

pip install pytest

# COMMAND ----------

from pyspark.sql.functions import min, max, avg, col
import pytest

# COMMAND ----------

def extract():
    df_cleansed = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Cleansed/")
    return df_cleansed

def transform(df_cleansed):
    # Calculate summary statistics
    df_summary = df_cleansed.groupBy("turbine_id").agg(
        min("power_output").alias("min_power_output"),
        max("power_output").alias("max_power_output"),
        avg("power_output").alias("avg_power_output")
    )
    mean_val, stddev_val = calculate_mean_stddev(df_cleansed)
    
    # Identify anomalies: turbines whose output is outside 2 standard deviations
    df_anomalies = df_cleansed.withColumn(
        "anomaly", 
        (col("power_output") < mean_val - 2 * stddev_val) | 
        (col("power_output") > mean_val + 2 * stddev_val)
    ).filter(col("anomaly") == True)

    return df_cleansed, df_summary, df_anomalies

def load(df_cleansed, df_summary, df_anomalies):
    # Write the data in processed layer
    df_cleansed.write.format("delta").mode("overwrite").partitionBy("turbine_id").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/cleansed_data/")
    df_summary.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/summary_stats/")
    df_anomalies.write.format("delta").mode("overwrite").partitionBy("turbine_id").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/anomalies/")

    # Use Delta Lake’s optimize feature to compact small files into fewer large files for better query performance
    optimize_and_zorder("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/cleansed_data/", "timestamp")
    optimize_and_zorder("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/summary_stats/", "turbine_id")
    optimize_and_zorder("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/anomalies/", "timestamp")

def test_data_cleansing(df_cleaned):
    assert df_cleaned.filter(col("power_output").isNull()).count() == 0

def test_anomalies_identification(df_anomalies):
    anomalies_count = df_anomalies.count()
    assert anomalies_count == 0, "Aanomalies detected!"
    

# COMMAND ----------

df = extract()

df_cleansed, df_summary, df_anomalies = transform(df)

load(df_cleansed, df_summary, df_anomalies)

# COMMAND ----------

test_data_cleansing(df_cleansed)
test_anomalies_identification(df_anomalies)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a database if not already created
# MAGIC CREATE DATABASE IF NOT EXISTS energy_db;

# COMMAND ----------

# Save DataFrames as permanent Delta tables
df_cleansed.write.format("delta").mode("overwrite").saveAsTable("energy_db.cleansed_data")
df_summary.write.format("delta").mode("overwrite").saveAsTable("energy_db.summary_data")
df_anomalies.write.format("delta").mode("overwrite").saveAsTable("energy_db.anomalies_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_db.cleansed_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_db.summary_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_db.anomalies_data