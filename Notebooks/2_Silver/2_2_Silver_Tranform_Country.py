# Databricks notebook source
spark.sql("DROP TABLE IF EXISTS our_world_in_data.silver_layer.country;")
spark.sql("""
    CREATE TABLE IF NOT EXISTS our_world_in_data.silver_layer.country(
    Country_id BIGINT GENERATED ALWAYS AS IDENTITY,
    CountryName VARCHAR(300),
    CountryIsoCode VARCHAR(3),
    CountryRowHash VARCHAR(100)
    );
""")

# COMMAND ----------

spark.sql("""
    MERGE INTO our_world_in_data.silver_layer.country AS target
    USING (
        SELECT DISTINCT country, iso_code FROM our_world_in_data.bronze_layer.energy 
        WHERE iso_code IS NOT NULL
        order by country
    ) AS source
    ON target.CountryIsoCode = source.iso_code
    WHEN NOT MATCHED THEN
        INSERT (CountryName, CountryIsoCode, CountryRowHash)
        VALUES (source.country, source.iso_code, hash(source.country, source.iso_code));
""")

# COMMAND ----------

# spark.sql("SELECT * FROM our_world_in_data.silver_layer.country").display()