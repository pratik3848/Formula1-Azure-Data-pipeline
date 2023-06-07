# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Find race years for which data needs to be reprocessed

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_list = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()


# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []

for val in race_results_list:
    race_year_list.append(val.race_year)

# COMMAND ----------

race_results_df = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

driver_standing_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("Wins")) \
.withColumn("rank", rank().over(Window.partitionBy("race_year").orderBy(desc("total_points"), desc("Wins"))))

# COMMAND ----------

display(driver_standing_df)

# COMMAND ----------


# overwrite_partition(driver_standing_df, 'f1_presentation', 'driver_standings', 'race_year')

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(driver_standing_df, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2021

# COMMAND ----------

