# Databricks notebook source
import sys
import os
import requests
import Module.DataFrame_Functions as dff

# COMMAND ----------

spark.sql("USE CATALOG our_world_in_data")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_layer")
spark.sql("CREATE VOLUME IF NOT EXISTS our_world_in_data.bronze_layer.landing_zone;")

# COMMAND ----------

catalog = "our_world_in_data"
schema = "bronze_layer"
volume = "landing_zone"
file_name = "energy_data.csv"
path_volume = f"/Volumes/{catalog}/{schema}/{volume}/{file_name}"
url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'

# COMMAND ----------

# DBTITLE 1,Cell 4
response = requests.get(url)

if response.status_code == 200:
    dbutils.fs.put(path_volume, response.content.decode('utf-8'), overwrite=True)

# COMMAND ----------

df = spark.read.csv(path_volume, header=True, inferSchema=None)
df.display()

# COMMAND ----------

energy_bronze_df = dff.create_ingestions_columns(df, 'gitrawcontent')
energy_bronze_df.display()

# COMMAND ----------

energy_bronze_df.printSchema()

# COMMAND ----------

(energy_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("our_world_in_data.bronze_layer.energy"))