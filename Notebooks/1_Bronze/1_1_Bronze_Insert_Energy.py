# Databricks notebook source

# Importa a biblioteca requests para fazer requisições HTTP
import requests
# Importa funções personalizadas para manipulação de DataFrames
import Module.DataFrame_Functions as dff

# COMMAND ----------
# URL do dataset de energia disponível publicamente no GitHub
url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'

# COMMAND ----------
# Faz o download do arquivo CSV usando uma requisição HTTP
response = requests.get(url)

# COMMAND ----------
# Salva o conteúdo baixado como um arquivo temporário no DBFS
dbutils.fs.put("/tmp/energy_data.csv", response.text, overwrite=True)

# COMMAND ----------
# Lê o arquivo CSV salvo como um DataFrame Spark, inferindo o schema e usando o cabeçalho
df = spark.read.csv("/tmp/energy_data.csv", header=True, inferSchema=True)

# COMMAND ----------
# Aplica função personalizada para adicionar colunas de ingestão ao DataFrame
energy_bronze_df = dff.create_ingestions_columns(df, 'gitrawcontent')

# COMMAND ----------
# Escreve o DataFrame processado em uma tabela Delta no Databricks, camada bronze
(energy_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("our_world_in_data.bronze_layer.energy"))