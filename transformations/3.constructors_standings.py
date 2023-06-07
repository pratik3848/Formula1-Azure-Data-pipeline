# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

race_results_list = spark.read \
.format("delta") \
.load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()


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

constructors_standing_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), sum(when(col("position") == 1, 1)).alias("Wins")) \
.withColumn("rank", rank().over(Window.partitionBy("race_year").orderBy(desc("total_points"), desc("Wins"))))

# COMMAND ----------

display(constructors_standing_df)

# COMMAND ----------

# constructors_standing_df.write \
# .mode("overwrite") \
# .format("parquet") \
# .saveAsTable('f1_presentation.constructors_standings')

# overwrite_partition(constructors_standing_df, 'f1_presentation', 'constructor_standings', 'race_year')


merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(constructors_standing_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings WHERE race_year = 2021

# COMMAND ----------

