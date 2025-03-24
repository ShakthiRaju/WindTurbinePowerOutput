# Databricks notebook source
# MAGIC %run /Users/shakthibiz16@gmail.com/WindTurbine/HelperFunctions

# COMMAND ----------

from pyspark.sql.functions import mean, stddev
from pyspark.sql.functions import col

# COMMAND ----------

def extract():
    df_source = spark.read.format("delta").load("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Source/")
    return df_source

def transform(df_source):
    # Handle Missing Values
    df_cleansed = df_source.fillna({  "wind_speed": 0, "wind_direction": 0,"power_output": 0})

    # Remove Outliers
    mean_val, stddev_val = calculate_mean_stddev(df_cleansed)

    df_cleansed = df_cleansed.filter((col("power_output") >= mean_val - 2 * stddev_val) & (col("power_output") <= mean_val + 2 * stddev_val))

    return df_cleansed

def load(df_cleansed):
    df_cleansed.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Cleansed/")

# COMMAND ----------

df = extract()

df = transform(df)

load(df)

# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com//WindTurbine/Cleansed/")