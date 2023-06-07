-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1dl3848.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_processed

-- COMMAND ----------

