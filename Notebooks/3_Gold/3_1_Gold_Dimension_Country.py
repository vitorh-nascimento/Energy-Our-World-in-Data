# Databricks notebook source
spark.sql("""
            CREATE TABLE IF NOT EXISTS our_world_in_data.gold_layer.dim_country(
                CountrySK BIGINT GENERATED ALWAYS AS IDENTITY,
                CountryName VARCHAR(300),
                CountryIsoCode VARCHAR(3),
                CountryRowHash VARCHAR(100)
            );
          """)

# COMMAND ----------

spark.sql("""
            MERGE INTO our_world_in_data.gold_layer.dim_country as trg
            USING our_world_in_data.silver_layer.country as src
            ON trg.CountryName = src.CountryName  
            WHEN MATCHED AND trg.CountryRowHash <> src.CountryRowHash 
                THEN UPDATE SET 
                    trg.CountryIsoCode = src.CountryIsoCode,
                    trg.CountryRowHash = src.CountryRowHash
            WHEN NOT MATCHED 
                THEN INSERT (CountryName, CountryIsoCode, CountryRowHash)
                    VALUES (src.CountryName, src.CountryIsoCode, src.CountryRowHash);
          """)

# COMMAND ----------

spark.sql("SELECT * from our_world_in_data.gold_layer.dim_country").display()