from pyspark.sql import functions as sf
import uuid

def create_ingestions_columns(df, source):
    """
    Adiciona colunas de metadados de ingestão ao DataFrame fornecido.

    Parâmetros:
    df (DataFrame): DataFrame Spark de entrada.
    source (str): Nome ou identificador da fonte de ingestão.

    Retorna:
    DataFrame: DataFrame com as colunas de ingestão adicionadas.
    """
    batch_id = str(uuid.uuid4())  # Gera um identificador único para o lote de ingestão

    return (df
        # Adiciona coluna com timestamp atual da ingestão
        .withColumn('_ingestion_date', sf.current_timestamp())
        # Adiciona coluna com o identificador único do lote
        .withColumn('_ingestion_batch_id', sf.lit(batch_id))
        # Adiciona coluna com o nome da fonte de ingestão
        .withColumn('_ingestion_source', sf.lit(source))
    )