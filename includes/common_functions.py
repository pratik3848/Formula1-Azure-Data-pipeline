# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_column(input_df, partition_column):
    partition_column = partition_column
    column_list = []
    
    for name in input_df.schema.names:
        if name != partition_column:
            column_list.append(name)
    column_list.append(partition_column)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    output_df = rearrange_partition_column(input_df, partition_column)
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write \
        .mode("overwrite") \
        .insertInto(f"{db_name}.{table_name}") #creates partition based on last column
    else:
        output_df.write \
        .mode("append") \
        .partitionBy(partition_column) \
        .format("parquet") \
        .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

#used for managed tables
#inserts entire df as table if table doesn't exist
#otherwise merges the source and target df in delta

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    from delta.tables import DeltaTable
    
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
            deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
            deltaTable.alias("tgt") \
            .merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write \
        .mode("append") \
        .partitionBy(partition_column) \
        .format("delta") \
        .saveAsTable(f"{db_name}.{table_name}") 

# COMMAND ----------

