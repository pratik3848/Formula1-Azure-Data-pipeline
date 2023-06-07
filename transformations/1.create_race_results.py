# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

races_df = spark.read \
.format("delta") \
.load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

constructors_df = spark.read \
.format("delta") \
.load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read \
.format("delta") \
.load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "results_race_id") \
.withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

drivers_df = spark.read \
.format("delta") \
.load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

circuits_df = spark.read \
.format("delta") \
.load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

joined_df = races_df \
.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.join(results_df, races_df.race_id == results_df.results_race_id, "inner" ) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap_time", "race_time", "points", "position", "race_id", "results_file_date") \
.withColumnRenamed("fastest_lap_time", "fastest_lap") \
.withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = joined_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

# display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results