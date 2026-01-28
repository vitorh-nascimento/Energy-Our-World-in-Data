# Databricks notebook source

# Cria a tabela 'country' na camada silver, se não existir, com colunas para id, nome, código ISO e hash da linha.
spark.sql("""
            DROP TABLE IF EXISTS our_world_in_data.silver_layer.country;

            CREATE TABLE IF NOT EXISTS our_world_in_data.silver_layer.country(
            Country_id BIGINT GENERATED ALWAYS AS IDENTITY,
            CountryName VARCHAR(300),
            CountryIsoCode VARCHAR(3),
            CountryRowHash VARCHAR(100)
            );
          """)

# COMMAND ----------
# Faz merge dos países distintos da camada bronze na tabela 'country' da camada silver.
# Insere novos países que ainda não existem na tabela, gerando um hash para cada linha.
spark.sql("""
            MERGE INTO our_world_in_data.silver_layer.country AS target
            USING (
                SELECT DISTINCT country, iso_code FROM our_world_in_data.bronze_layer.energy 
                WHERE iso_code IS NOT NULL
            ) AS source
            ON target.CountryIsoCode = source.iso_code
            WHEN NOT MATCHED THEN
            INSERT (CountryName, CountryIsoCode, CountryRowHash)
            VALUES (source.country, source.iso_code, hash(source.country, source.iso_code));
          """)

# COMMAND ----------
# Seleciona todos os registros da tabela 'country' para visualização.
spark.sql("""
            SELECT * FROM our_world_in_data.silver_layer.country
          """)