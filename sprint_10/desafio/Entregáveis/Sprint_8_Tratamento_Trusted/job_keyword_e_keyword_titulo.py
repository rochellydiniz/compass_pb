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
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler os dados da RAW Zone
caminho_entrada_s3_filmes = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/keywords_filmes/2025/02/15/"
caminho_entrada_s3_series = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/keywords_series/2025/03/03/"

df_keywords_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_keywords_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

ano = "2025"
mes = "02"
dia = "15"

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
