import sys
import boto3
import builtins
import re
from math import ceil
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, explode, lit, coalesce, when, collect_list, concat_ws
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Parâmetro do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializar o Spark e Glue Context
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
caminho_entrada_s3_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/creditos_filmes/")
caminho_entrada_s3_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/creditos_series/")

# Carregar os dados
df_creditos_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_creditos_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

# Extração de ano, mês e dia do caminho encontrado
ano, mes, dia = caminho_entrada_s3_filmes.split("/")[-4], caminho_entrada_s3_filmes.split("/")[-3], caminho_entrada_s3_filmes.split("/")[-2]


# Processar df_creditos
df_elenco_filmes = df_creditos_filmes.withColumn("elenco", explode(col("elenco_principal"))).select(
    col("movie_id").cast(IntegerType()).alias("id_titulo"),
    lit("F").cast(StringType()).alias("tp_titulo"),
    coalesce(col("elenco.id"), lit(0)).cast(IntegerType()).alias("id_pessoa"),
    
    when(col("elenco.personagem").isNull(), lit("N/A")) \
    .when(col("elenco.personagem") == "", lit("N/A")) \
    .otherwise(col("elenco.personagem")) \
    .cast(StringType()) \
    .alias("nm_personagem")
)

df_elenco_series = df_creditos_series.withColumn("elenco", explode(col("elenco_principal"))).select(
    col("movie_id").cast(IntegerType()).alias("id_titulo"),
    lit("S").cast(StringType()).alias("tp_titulo"),
    coalesce(col("elenco.id"), lit(0)).cast(IntegerType()).alias("id_pessoa"),

    when(col("elenco.personagem").isNull(), lit("N/A")) \
    .when(col("elenco.personagem") == "", lit("N/A")) \
    .otherwise(col("elenco.personagem")) \
    .cast(StringType()) \
    .alias("nm_personagem")
)

# Unir os DF de filmes e series
df_elenco = df_elenco_filmes.unionByName(df_elenco_series)
df_elenco = df_elenco.groupBy("id_titulo", "tp_titulo", "id_pessoa") \
    .agg(concat_ws(" || ", collect_list("nm_personagem")).alias("nm_personagem"))

df_elenco = df_elenco \
    .withColumn("nr_ano", lit(ano).cast(StringType())) \
    .withColumn("nr_mes", lit(mes).cast(StringType())) \
    .withColumn("nr_dia", lit(dia).cast(StringType()))

df_elenco.show(5)
df_elenco.printSchema()


# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_elenco.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3 = f"s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_elenco/"
df_elenco.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

job.commit()