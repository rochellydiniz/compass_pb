# Job Glue para junção de generos de filmes e series e tratamento de dados

import sys
import boto3
import re
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode, when, lit
from pyspark.sql.types import StringType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')
nome_bucket = "desafio-final.data-lake"

# Função para encontrar a partição mais recente em um diretório
def obter_ultima_particao(bucket, prefixo_base):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_base)

    if "Contents" not in response or not response["Contents"]:
        print(f"enhuma partição encontrada para {prefixo_base}!")
        return None

    datas_encontradas = []
    for obj in response["Contents"]:
        caminho = obj["Key"].split("/")
        try:
            ano, mes, dia = int(caminho[-4]), int(caminho[-3]), int(caminho[-2])
            data = datetime(ano, mes, dia)
            datas_encontradas.append((data, f"s3://{bucket}/{prefixo_base}{ano}/{mes:02d}/{dia:02d}/"))
        except (ValueError, IndexError):
            continue

    if not datas_encontradas:
        print(f"Nenhuma partição válida encontrada para {prefixo_base}!")
        return None

    ultima_particao = max(datas_encontradas, key=lambda x: x[0])[1]
    return ultima_particao


# Ler os dados da RAW Zone
caminho_entrada_s3_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/genres_filmes/")
caminho_entrada_s3_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/genres_series/")

df_genero_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_genero_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

ano, mes, dia = caminho_entrada_s3_filmes.split("/")[-4], caminho_entrada_s3_filmes.split("/")[-3], caminho_entrada_s3_filmes.split("/")[-2]

# Explodir array de gêneros e renomear colunas
if "genres" in df_genero_filmes.columns:
    df_genero_filmes = df_genero_filmes.withColumn("genre", explode(col("genres"))).select(
                        col("genre.id").cast(IntegerType()).alias("id_genero"),
                        col("genre.name").cast(StringType()).alias("nm_genero")
    )

if "genres" in df_genero_series.columns:
    df_genero_series = df_genero_series.withColumn("genre", explode(col("genres"))).select(
                    col("genre.id").cast(IntegerType()).alias("id_genero"),
                    col("genre.name").cast(StringType()).alias("nm_genero")

    )

# Unir as tabelas de gêneros de filmes e séries
df_genero = df_genero_filmes.union(df_genero_series).dropDuplicates(["id_genero"])

# Adicionar colunas de partição
df_genero = df_genero.withColumn("nr_ano", lit(ano))
df_genero = df_genero.withColumn("nr_mes", lit(mes))
df_genero = df_genero.withColumn("nr_dia", lit(dia))


df_genero.show()
df_genero.printSchema()

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3= f"s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_genero/"
df_genero.write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

print(f"Salvo com sucesso em {caminho_saida_s3}")

job.commit()

