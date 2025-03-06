import sys
from math import ceil
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.functions import col



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Ler os dados da RAW Zone
caminho_entrada_s3_filmes = "s3://desafio-final.data-lake/RAW Zone/Local/CSV/Filmes/2025/01/03/movies.csv"
caminho_entrada_s3_series = "s3://desafio-final.data-lake/RAW Zone/Local/CSV/Series/2025/01/03/series.csv"

df_filmes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", "|").load(caminho_entrada_s3_filmes)
df_series = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", "|").load(caminho_entrada_s3_series)

# Renomear colunas filmes
df_filmes_local = df_filmes.select(
    F.coalesce(F.col("id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
    F.lit("F").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("tituloPincipal"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    F.coalesce(F.col("tituloOriginal"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    F.coalesce(F.col("anoLancamento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_lancamento"),
    F.coalesce(F.col("tempominutos"), F.lit(0)).cast(IntegerType()).alias("tm_minutos"),
    F.coalesce(F.col("genero"), F.lit("N/A")).cast(StringType()).alias("nm_genero"),
    F.coalesce(F.col("notaMedia"), F.lit(0.0)).cast(DoubleType()).alias("vl_nota_media"),
    F.coalesce(F.col("numeroVotos"), F.lit(0)).cast(IntegerType()).alias("qt_votos"),
    F.coalesce(F.col("generoArtista"), F.lit("N/A")).cast(StringType()).alias("tp_sexo"),
    F.coalesce(F.col("personagem"), F.lit("N/A")).cast(StringType()).alias("nm_personagem"),
    F.coalesce(F.col("nomeArtista"), F.lit("N/A")).cast(StringType()).alias("nm_pessoa"),
    F.coalesce(F.col("anoNascimento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_nascimento"),
    F.coalesce(F.col("anoFalecimento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_falecimento"),
)

# Renomear colunas series
df_series_local = df_series.select(
    F.coalesce(F.col("id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
    F.lit("S").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("tituloPincipal"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    F.coalesce(F.col("tituloOriginal"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    F.coalesce(F.col("anoLancamento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_lancamento"),
    F.coalesce(F.col("tempominutos"), F.lit(0)).cast(IntegerType()).alias("tm_minutos"),
    F.coalesce(F.col("genero"), F.lit("N/A")).cast(StringType()).alias("nm_genero"),
    F.coalesce(F.col("notaMedia"), F.lit(0.0)).cast(DoubleType()).alias("vl_nota_media"),
    F.coalesce(F.col("numeroVotos"), F.lit(0)).cast(IntegerType()).alias("qt_votos"),
    F.coalesce(F.col("generoArtista"), F.lit("N/A")).cast(StringType()).alias("tp_sexo"),
    F.coalesce(F.col("personagem"), F.lit("N/A")).cast(StringType()).alias("nm_personagem"),
    F.coalesce(F.col("nomeArtista"), F.lit("N/A")).cast(StringType()).alias("nm_pessoa"),
    F.coalesce(F.col("anoNascimento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_nascimento"),
    F.coalesce(F.col("anoFalecimento"), F.lit(1700)).cast(IntegerType()).alias("dt_ano_falecimento"),
)

# Tratar valores nulos
df_filmes_local = df_filmes_local. \
    withColumn("dt_ano_lancamento", F.when(F.col("dt_ano_lancamento").isNull(),1700).otherwise(F.col("dt_ano_lancamento"))). \
    withColumn("dt_ano_nascimento", F.when(F.col("dt_ano_nascimento").isNull(),1700).otherwise(F.col("dt_ano_nascimento"))). \
    withColumn("dt_ano_falecimento", F.when(F.col("dt_ano_falecimento").isNull(),1700).otherwise(F.col("dt_ano_falecimento")))



df_series_local = df_series_local. \
    withColumn("dt_ano_lancamento", F.when(F.col("dt_ano_lancamento").isNull(),1700).otherwise(F.col("dt_ano_lancamento"))). \
    withColumn("dt_ano_nascimento", F.when(F.col("dt_ano_nascimento").isNull(),1700).otherwise(F.col("dt_ano_nascimento"))). \
    withColumn("dt_ano_falecimento", F.when(F.col("dt_ano_falecimento").isNull(),1700).otherwise(F.col("dt_ano_falecimento")))


# Unir dataframes
df_titulo = df_filmes_local.unionByName(df_series_local, allowMissingColumns=True)

# Filtrar títulos com gêneros de interesse
df_titulo_local = df_titulo.filter(
    (col("nm_genero").contains("Horror")) | (col("nm_genero").contains("Mystery"))
)

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_titulo_local.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

# Salvar os dados em formato Parquet na camada processada do Data Lake
caminho_saida_s3 = "s3://desafio-final.data-lake/Trusted/Local/Parquet/tb_titulo_local/"
df_titulo_local.repartition(qtd_arquivos).write.mode('overwrite').option('compression', 'snappy').parquet(caminho_saida_s3)

job.commit()