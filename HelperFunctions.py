# Databricks notebook source
from pyspark.sql.functions import mean, stddev

# COMMAND ----------

def calculate_mean_stddev(df):
    stats = df.select(mean("power_output").alias("mean"), stddev("power_output").alias("stddev")).collect()[0]
    mean_val, stddev_val = stats["mean"], stats["stddev"]
    return mean_val, stddev_val


def optimize_and_zorder(delta_path, column_name):
    spark.sql(f'OPTIMIZE delta.`{delta_path}` ZORDER BY {column_name}')