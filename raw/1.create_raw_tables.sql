-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "abfss://raw@formula1dl3848.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)         
USING csv
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forname: STRING, surname STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results
(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText INT,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/results.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/pit_stops.json", multiLine true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
raceId INT,
driverId STRING,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/lap_times")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
constructorId INT,
driverId STRING,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
USING json
OPTIONS(path "abfss://raw@formula1dl3848.dfs.core.windows.net/qualifying" , multiLine true)

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying

-- COMMAND ----------

