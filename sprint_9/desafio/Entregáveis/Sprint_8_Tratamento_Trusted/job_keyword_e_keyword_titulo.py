# Job Glue para juntar palavras-chave de filmes e series e tratar os dados

import sys
import boto3
import re
from datetime import datetime
from math import ceil
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuração do cliente S3
s3_client = boto3.client('s3')
nome_bucket = "desafio-final.data-lake"

# Função para encontrar a última partição disponível no S3
def obter_ultima_particao(bucket, prefixo_base):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_base)

    if "Contents" not in response or not response["Contents"]:
        print(f"Nenhuma partição encontrada para {prefixo_base}!")
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

# Buscar dinamicamente a última partição disponível
caminho_entrada_s3_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/keywords_filmes/")
caminho_entrada_s3_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/keywords_series/")

# Carregar os dados
df_keywords_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_keywords_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

# Extração de ano, mês e dia do caminho encontrado
ano, mes, dia = caminho_entrada_s3_filmes.split("/")[-4], caminho_entrada_s3_filmes.split("/")[-3], caminho_entrada_s3_filmes.split("/")[-2]

# Tratar e renomear colunas

df_keyword_filmes = df_keywords_filmes.select(F.col("movie_id"), F.explode("keywords").alias("keywords")) \
                .select(
                    F.col("movie_id").cast(IntegerType()).alias("id_titulo"),
                    F.lit("F").cast(StringType()).alias("tp_titulo"),
                    F.coalesce(F.col("keywords.id"), F.lit(0)).cast(IntegerType()).alias("id_keyword"),
                    F.coalesce(F.col("keywords.name"), F.lit("N/A")).cast(StringType()).alias("ds_keyword")
                )

df_keyword_series = df_keywords_series.select(F.col("id"), F.explode("results").alias("keywords")) \
               .select(
                   F.col("id").cast(IntegerType()).alias("id_titulo"),
                   F.lit("F").cast(StringType()).alias("tp_titulo"),
                   F.coalesce(F.col("keywords.id"), F.lit(0)).alias("id_keyword"),
                   F.coalesce(F.col("keywords.name"), F.lit("N/A")).alias("ds_keyword")
               )

# Unir dfs
df_keywords = df_keyword_filmes.unionByName(df_keyword_series)

# Excluir coluna id_titulo e tp_titulo e deixar apenas id_keyword distintos
df_keyword = df_keywords.drop("tp_titulo", "id_titulo")
df_keyword = df_keyword.distinct()


df_keyword = df_keyword \
    .withColumn("nr_ano", F.lit(ano).cast(StringType())) \
    .withColumn("nr_mes", F.lit(mes).cast(StringType())) \
    .withColumn("nr_dia", F.lit(dia).cast(StringType()))

df_keyword.show(10)
df_keyword.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_keyword.count()
qtd_arquivos = ceil(total_registros / 200) if total_registros > 200 else 1

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3 = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_keyword/"
df_keyword.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

print(f"Salvo com sucesso em {caminho_saida_s3}")

###############################################
# Criar df_assoc_keyword_titulo

df_assoc_keyword_titulo = df_keywords.select(
    col("id_titulo"),
    col("tp_titulo"),
    col("id_keyword")
)

df_assoc_keyword_titulo = df_assoc_keyword_titulo \
    .withColumn("nr_ano", F.lit(ano).cast(StringType())) \
    .withColumn("nr_mes", F.lit(mes).cast(StringType())) \
    .withColumn("nr_dia", F.lit(dia).cast(StringType()))

df_assoc_keyword_titulo.show(5)   
df_assoc_keyword_titulo.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_assoc_keyword_titulo.count()
qtd_arquivos = ceil(total_registros / 200) if total_registros > 200 else 1

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3 = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_assoc_keyword_titulo/"
df_assoc_keyword_titulo.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

print(f"Salvo com sucesso em {caminho_saida_s3}")

job.commit()
