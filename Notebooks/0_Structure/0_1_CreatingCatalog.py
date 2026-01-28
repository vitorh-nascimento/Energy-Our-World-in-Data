# Databricks notebook source

# Cria o catálogo 'our_world_in_data' se não existir e define-o como catálogo ativo.
spark.sql("""
            CREATE CATALOG IF NOT EXISTS our_world_in_data;
            USE CATALOG our_world_in_data;
          """)

# COMMAND ----------

# Cria os schemas (camadas) bronze, silver e gold, cada um com um comentário explicativo:
# - bronze_layer: dados brutos ingeridos diretamente da fonte
# - silver_layer: dados tratados e filtrados
# - gold_layer: dados agregados e modelados, prontos para tomada de decisão
spark.sql("""
            CREATE SCHEMA IF NOT EXISTS bronze_layer
            COMMENT 'Dados brutos ingeridos diretamente da fonte';
            CREATE SCHEMA IF NOT EXISTS silver_layer
            COMMENT 'Dados tratados e filtrados';
            CREATE SCHEMA IF NOT EXISTS gold_layer
            COMMENT 'Dados agregados e modelados, prontos para tomada de decisão';
          """)