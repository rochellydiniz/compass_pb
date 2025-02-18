# Job Glue para juntar palavras-chave de filmes e series e tratar os dados

import sys
from datetime import datetime
from math import ceil
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode, when
from pyspark.sql.types import StringType, IntegerType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler os dados da RAW Zone
bucket_name = "desafio-final.data-lake"

frame_filmes = glueContext.create_dynamic_frame.from_catalog(
    database = "desafio.final-raw.tmdb.data",
    table_name = "keywords_filmes",
    transformation_ctx="input_data"
)

frame_series = glueContext.create_dynamic_frame.from_catalog(
    database = "desafio.final-raw.tmdb.data",
    table_name = "keywords_series",
    transformation_ctx="input_data"    
)

# Renomear colunas
colunas = {
    "id": "id_keyword",
    "name": "nm_keyword",
    "partition_0": "ano",
    "partition_1": "mes",
    "partition_2": "dia"
}

for old_name, new_name in colunas.items():
    frame_filmes = frame_filmes.rename_field(old_name, new_name)
    frame_series = frame_series.rename_field(old_name, new_name)

# Tratar os dados
frame_filmes_tratado = frame_filmes.resolveChoice(specs=[
    ('ano', 'cast:int'),
    ('mes', 'cast:int'),
    ('dia', 'cast:int')
    ])
    
frame_series_tratado = frame_series.resolveChoice(specs=[
    ('ano', 'cast:int'),
    ('mes', 'cast:int'),
    ('dia', 'cast:int')
    ])

# Converter DynamicFrame para Spark DataFrame
df_keywords_filmes = frame_filmes_tratado.toDF()
df_keywords_series = frame_series_tratado.toDF()

colunas_esperadas = ["id_movie", "id_keyword", "nm_keyword", "ano", "mes", "dia"]

for coluna in colunas_esperadas:
    if coluna not in df_keywords_filmes.columns:
        df_keywords_filmes = df_keywords_filmes.withColumn(coluna, F.lit(None).cast(StringType()))
    if coluna not in df_keywords_series.columns:
        df_keywords_series = df_keywords_series.withColumn(coluna, F.lit(None).cast(StringType()))        

df_keywords_filmes = df_keywords_filmes.select(colunas_esperadas)
df_keywords_series = df_keywords_series.select(colunas_esperadas)

# Excluir coluna id_movie
df_keywords_filmes = df_keywords_filmes.drop("id_movie")
df_keywords_series = df_keywords_series.drop("id_movie")

# Unir as tabelas de filmes e series
df_keywords_final = df_keywords_filmes.union(df_keywords_series).dropDuplicates(["id_keyword"])

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_keywords_final.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

# Salvar os dados em formato Parquet na camada trusted
output_path = f"s3://{bucket_name}/Trusted/TMDB/Parquet/Keywords/"
df_keywords_final.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("ano", "mes", "dia").option('compression', 'snappy').parquet(output_path)

print(f"Salvo com sucesso em {output_path}")

job.commit()
