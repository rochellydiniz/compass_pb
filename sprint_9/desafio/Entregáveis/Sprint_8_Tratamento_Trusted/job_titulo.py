import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper, coalesce, lit
from pyspark.sql.types import StringType
from math import ceil

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados
caminho_entrada_s3_tmdb = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_titulo_tmdb/nr_ano=2025/nr_mes=02/nr_dia=08/"
caminho_entrada_s3_local = "s3://desafio-final.data-lake/Trusted/Local/Parquet/tb_titulo_local/"

df_tmdb = spark.read.parquet(caminho_entrada_s3_tmdb)
df_local = spark.read.parquet(caminho_entrada_s3_local)

ano, mes, dia = "2025", "02", "08"

# Selecionar colunas e tratar nulos
df_tmdb = df_tmdb.select(
    col("id_titulo"),
    col("tp_titulo"),
    col("nm_titulo_principal"),
    col("sg_pais_origem"),
    col("sg_idioma"),
    col("vl_popularidade_titulo"),
    col("dt_lancamento"),
    col("ds_sinopse"),
    col("id_imdb")
)

df_local = df_local.select(
    coalesce(col("id_imdb"), lit("N/A")).alias("id_imdb_local"),
    coalesce(col("nm_titulo_original"), lit("N/A")).alias("nm_titulo_original"),
    coalesce(col("vl_nota_media"), lit(0.0)).alias("vl_nota_media"),
    coalesce(col("qt_votos"), lit(0)).alias("qt_votos"),
).distinct()

# Join nos DataFrames pelo id_imdb
df_titulo = df_tmdb.join(df_local, df_tmdb['id_imdb'] == df_local['id_imdb_local'], 'left')

# Tratar nulos
df_titulo = df_titulo.na.fill("N/A", ["id_imdb_local","nm_titulo_original"])
df_titulo = df_titulo.na.fill(0.0, ["vl_nota_media"])
df_titulo = df_titulo.na.fill(0, ["qt_votos"])

# Corrigir ordenação das colunas e incluir upper

df_titulo = df_titulo.select(
    col("id_titulo"),
    col("tp_titulo"),
    col("nm_titulo_principal"),
    col("nm_titulo_original"),
    col("dt_lancamento"),
    col("sg_pais_origem"),
    upper(col("sg_idioma")).alias("sg_idioma"),
    col("vl_popularidade_titulo"),
    col("vl_nota_media"),
    col("qt_votos"),
    col("ds_sinopse"),
)

df_titulo = df_titulo \
    .withColumn("nr_ano", lit(ano).cast(StringType())) \
    .withColumn("nr_mes", lit(mes).cast(StringType())) \
    .withColumn("nr_dia", lit(dia).cast(StringType()))

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_titulo.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

df_titulo.show(5)
df_titulo.printSchema()

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3 = f"s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_titulo/"
df_titulo.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

job.commit()