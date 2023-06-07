# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("year", IntegerType(), True),
                                       StructField("round", IntegerType(), True),
                                       StructField("circuitId", IntegerType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("date", DateType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), 
                                          col("circuitId"), col("name"), col("date"),
                                          col("time"))

# COMMAND ----------

 races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), 'time'), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

cols = ("date","time")
races_final_df = races_final_df.drop(*cols)

# COMMAND ----------

races_final_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")