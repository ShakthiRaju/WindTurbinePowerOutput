# Databricks notebook source
# MAGIC %md
# MAGIC # Wind Turbine Power Output

# COMMAND ----------

# MAGIC %md
# MAGIC ## High Level Architecture :
# MAGIC
# MAGIC The high-level architecture provides a overview of the data pipeline for processing wind turbine data. It focuses on the main component and data flow.
# MAGIC
# MAGIC ### Components:
# MAGIC
# MAGIC #### 1. Data Sources : 
# MAGIC - CSV Files: Data from turbines is updated daily as an incremental load.
# MAGIC - The raw data can be stored in the Azure Data Lake Gen 2 but here for this implementation the data is stored in the DBFS due to limited access.
# MAGIC
# MAGIC #### 2. Ingestion Layer:
# MAGIC - Databricks Notebook : Read raw data from the DBFS storage.
# MAGIC
# MAGIC #### 3. Processing Layer:
# MAGIC - Data Cleaning: Handle missing values and remove outliers.
# MAGIC - Analytics:
# MAGIC     1. Summary Statistics (min, max, average(mean))
# MAGIC     2. Anomaly detection (based on standard deviation)
# MAGIC -  Transformation: Structured and cleansed data is prepared for downstream storage.
# MAGIC
# MAGIC #### 4. Storage Layer:
# MAGIC - Raw Data : Stored in Raw Zone in delta lake raw tables for traceability.
# MAGIC - Cleansed Data : Stored in a Processed Zone for analytics and reporting.
# MAGIC - Anomalies : Stored seperately in a processed Zone for alerting and auditing.
# MAGIC
# MAGIC #### 5. Consumption Layer:
# MAGIC - Databricks SQL, BI Tools like Power BI , Tableau for data visualization.
# MAGIC - Reports for turbine performance and anomaly analysis.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Low-Level Design
# MAGIC The low-level design details the implementation specifics, including code modules, transformations, and storage mechanisms.
# MAGIC
# MAGIC ###Modules:
# MAGIC #### 1. Ingestion Module:
# MAGIC - Reads CSV files using PySpark.
# MAGIC - Appends new data to raw storage.
# MAGIC
# MAGIC #### 2. Cleaning Module:
# MAGIC - Handles missing values using default imputation.
# MAGIC - Identifies and removes outliers using threshold 
# MAGIC
# MAGIC #### 3. Analytics Module:
# MAGIC - Calculates summary statistics:
# MAGIC - Min, max, mean for power output per turbine.
# MAGIC - Detects anomalies:
# MAGIC - Writes anomalies to a separate table.
# MAGIC
# MAGIC #### 4. Storage Module:
# MAGIC Writes:
# MAGIC - Raw data: Delta Lake table (energy_db.raw_data).
# MAGIC - Cleaned data: Delta Lake table (energy_db.cleansed_data).
# MAGIC - Summary statistics: Delta Lake table (energy_db.summary_data).
# MAGIC - Anomalies: Delta Lake table (energy_db.anomalies_data).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code Mapping
# MAGIC ### 1. Ingestion Module:
# MAGIC
# MAGIC `def ingest_data(file_paths):
# MAGIC
# MAGIC         raw_data = [spark.read.csv(path, header=True, inferSchema=True) for path in file_paths]
# MAGIC         df_raw = raw_data[0]
# MAGIC         for df in raw_data[1:]:
# MAGIC             df_raw = df_raw.union(df)
# MAGIC         df_raw.write.format("delta").mode("append").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Source/")
# MAGIC         return df_raw`
# MAGIC
# MAGIC ### 2. Cleaning Module:
# MAGIC
# MAGIC `def clean_data(df_raw):
# MAGIC
# MAGIC         # Handle missing values
# MAGIC         df_cleaned = df_raw.fillna({
# MAGIC             "wind_speed": 0, 
# MAGIC             "wind_direction": 0, 
# MAGIC             "power_output": 0
# MAGIC         })
# MAGIC         
# MAGIC         # Remove outliers
# MAGIC         stats = df_cleaned.select(mean("power_output").alias("mean"), stddev("power_output").alias("stddev")).collect()[0]
# MAGIC         mean_val, stddev_val = stats["mean"], stats["stddev"]
# MAGIC
# MAGIC         df_cleaned = df_cleaned.filter(
# MAGIC             (col("power_output") >= mean_val - 2 * stddev_val) &
# MAGIC             (col("power_output") <= mean_val + 2 * stddev_val)
# MAGIC         )
# MAGIC         
# MAGIC         df_cleaned.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/Cleansed/")
# MAGIC         return df_cleaned`
# MAGIC
# MAGIC ### 3. Analytics Module:
# MAGIC
# MAGIC `def calculate_summary(df_cleaned):
# MAGIC
# MAGIC         summary_df = df_cleaned.groupBy("turbine_id").agg(
# MAGIC             min("power_output").alias("min_power_output"),
# MAGIC             max("power_output").alias("max_power_output"),
# MAGIC             avg("power_output").alias("avg_power_output")
# MAGIC         )
# MAGIC         summary_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/summary_stats")
# MAGIC         return summary_df
# MAGIC
# MAGIC def detect_anomalies(df_cleaned):
# MAGIC
# MAGIC         anomalies_df = df_cleaned.withColumn(
# MAGIC             "anomaly", 
# MAGIC             (col("power_output") < mean_val - 2 * stddev_val) | 
# MAGIC             (col("power_output") > mean_val + 2 * stddev_val)
# MAGIC         ).filter(col("anomaly") == True)
# MAGIC         
# MAGIC         anomalies_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/shared_uploads/shakthibiz16@gmail.com/WindTurbine/processed_data/anomalies")
# MAGIC         return anomalies_df`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scalability
# MAGIC - Delta Lake: Ensures ACID compliance and scalability for large datasets.
# MAGIC - Partitioning: Partition by turbine_id for faster reads and writes.
# MAGIC - Job Scheduling: Use Databricks Workflows or Azure Data Factory for daily ingestion and processing.
# MAGIC
# MAGIC This design supports high data volumes, is testable and integrated well with existing tools for analytics and reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization 
# MAGIC
# MAGIC To improve the performance of the data ingestion and processing pipeline, given below the optimization techniques on various aspects of the architecture and implementation:
# MAGIC
# MAGIC ### 1. Optimize Data Storage
# MAGIC #### Use Delta Lake: 
# MAGIC - Delta Lake’s ACID transactions improve reliability and support schema evolution.
# MAGIC - Enables efficient updates and deletes, reducing data duplication.
# MAGIC #### Partitioning:
# MAGIC - Partition Delta tables by turbine_id to optimize queries and writes.
# MAGIC #### Data Compaction:
# MAGIC - Use Delta Lake’s optimize feature to compact small files into fewer large files for better query performance.
# MAGIC
# MAGIC ### 2. Optimize Data Ingestion
# MAGIC #### Use Auto Loader: 
# MAGIC - Databricks’ Auto Loader incrementally processes new files as they arrive.
# MAGIC
# MAGIC ### 3. Cache Intermediate Results
# MAGIC #### Cache Frequently Used Data:
# MAGIC - Cache cleaned or aggregated datasets for reuse during anomaly detection or reporting.
# MAGIC
# MAGIC ### 4. Monitor and Debug Performance
# MAGIC #### Enable Adaptive Query Execution (AQE) 
# MAGIC - Dynamically optimize query plans at runtime.
# MAGIC #### Use Spark UI:
# MAGIC - Monitor DAGs and identify bottlenecks in tasks or stages.
# MAGIC #### Logging:
# MAGIC - Add logging for timing and resource usage in each stage of the pipeline.
# MAGIC
# MAGIC By combining these techniques, throughput and latency of the pipeline can be enhanced. This approach ensured scalability as data volumes grows while maintaining performance.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document Assumption
# MAGIC - All turbine IDs are consistent in the same file group.
# MAGIC - Missing values in wind speed, direction, and power are filled with 0.
# MAGIC - Anomalies are identified strictly using 2 standard deviations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scale & Optimize
# MAGIC - Automate the pipeline with Databricks Workflows for daily execution.
# MAGIC - Using partitioning on turbine_id for faster queries.