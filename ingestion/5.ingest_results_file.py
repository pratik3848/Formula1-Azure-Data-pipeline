# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_schema = "resultId INT, raceId INT, driverId INT, \
constructorId INT, number INT, grid INT, position INT, \
positionText STRING, positionOrder INT, points FLOAT, \
laps INT, time STRING, milliseconds INT, fastestLap INT, \
rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT"

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positiontext", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date)) \
.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1 - Incremental Load

# COMMAND ----------

# for id in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {id.race_id})")

# COMMAND ----------

# results_final_df.write \
# .mode("append") \
# .partitionBy("race_id") \
# .format("parquet") \
# .saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 2 - Incremental Load

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   race_id,
# MAGIC   COUNT(*)
# MAGIC FROM 
# MAGIC   f1_processed.results
# MAGIC GROUP BY 
# MAGIC   race_id
# MAGIC ORDER BY
# MAGIC   race_id DESC;
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")