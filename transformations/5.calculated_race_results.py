# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
            (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
            )
            USING DELTA
""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW race_results_updates
AS 
SELECT
  t2.race_year,
  t4.name AS team_name,
  t3.driver_id,
  t3.name AS driver_name,
  t2.race_id,
  t1.position,
  t1.points ,
  11 - t1.position AS calculated_points
FROM
  f1_processed.results t1
JOIN
  f1_processed.races t2
ON
  t1.race_id = t2.race_id
JOIN
  f1_processed.drivers t3
ON
  t1.driver_id = t3.driver_id
JOIN
  f1_processed.constructors t4
ON
  t1.constructor_id = t4.constructor_id
WHERE
  t1.position <= 10 AND t1.file_date = '{v_file_date}'
""")
  

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM race_results_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updates upd
# MAGIC ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
# MAGIC WHEN
# MAGIC   MATCHED THEN UPDATE
# MAGIC                 SET tgt.position = upd.position, tgt.points = upd.points, tgt.calculated_points = upd.calculated_points, tgt.updated_date = current_timestamp
# MAGIC WHEN
# MAGIC   NOT MATCHED 
# MAGIC     THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date) 
# MAGIC          VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_results_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results

# COMMAND ----------

