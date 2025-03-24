# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("WindTurbinePipeline").getOrCreate()

# COMMAND ----------

def extract():
    # Defining file paths
    file_paths = [
        "dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Raw/data_group_1.csv",
        "dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Raw/data_group_2.csv",
        "dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Raw/data_group_3.csv"
    ]

    # Load data
    raw_data = [spark.read.csv(path, header=True, inferSchema=True) for path in file_paths]

    return raw_data

def transform(df):
    df_raw = df[0]
    for df in df[1:]:
        df_raw = df_raw.union(df)
    return df_raw

def load(df_raw):
    df_raw.write.format("delta").mode("append").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Source/")


# COMMAND ----------

df = extract()

df = transform(df)

load(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a database if not already created
# MAGIC CREATE DATABASE IF NOT EXISTS energy_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`dbfs:/user/hive/warehouse/energy_db.db/raw_data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS energy_db.raw_data 
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/user/hive/warehouse/energy_db.db/raw_data';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from energy_db.raw_data

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com//WindTurbine/Source/")