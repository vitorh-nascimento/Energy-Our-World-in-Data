# Databricks notebook source

# Cria a tabela dim_country no schema gold_layer, se ela não existir.
# CountrySK é uma chave substituta gerada automaticamente.
# CountryName: nome do país.
# CountryIsoCode: código ISO do país.
# CountryRowHash: hash da linha para controle de mudanças.
spark.sql("""
            CREATE TABLE IF NOT EXISTS our_world_in_data.gold_layer.dim_country(
                CountrySK BIGINT GENERATED ALWAYS AS IDENTITY,
                CountryName VARCHAR(300),
                CountryIsoCode VARCHAR(3),
                CountryRowHash VARCHAR(100)
            );
          """)

# COMMAND ----------
# Realiza o merge (upsert) dos dados da tabela silver_layer.country na tabela gold_layer.dim_country.
# Atualiza registros existentes se o hash mudou, ou insere novos registros se não houver correspondência.
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
# Exibe todos os registros da tabela dim_country.
spark.sql("""
            SELECT * from our_world_in_data.gold_layer.dim_country
          """)