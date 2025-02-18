# Job Glue para junção de generos de filmes e series e tratamento de dados

import sys
from datetime import datetime
from awsglue.transforms import Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Capturar a data de execução do Job
exec_date = datetime.now()
ano_exec = exec_date.year
mes_exec = exec_date.month
dia_exec = exec_date.day


# Ler os dados da RAW Zone
bucket_name = "desafio-final.data-lake"

frame_filmes = glueContext.create_dynamic_frame.from_catalog(
    database = "desafio.final-raw.tmdb.data",
    table_name = "genres_filmes",
)

frame_series = glueContext.create_dynamic_frame.from_catalog(
    database = "desafio.final-raw.tmdb.data",
    table_name = "genres_series",
)


# Converter DynamicFrame para Spark DataFrame
df_filmes = frame_filmes.toDF()
df_series = frame_series.toDF()

# Verificar a estrutura dos dados
df_filmes.printSchema()
df_series.printSchema()

# Explodir array de 
if "genres" in df_filmes.columns:
    df_filmes = df_filmes.withColumn("genre", explode(col("genres"))).select(
                    col("genre.id").cast(IntegerType()).alias("id_genre"),
                    col("genre.name").cast(StringType()).alias("nome_genre"),
                    F.lit(None).cast(IntegerType()).alias("ano"),
                    F.lit(None).cast(IntegerType()).alias("mes"),
                    F.lit(None).cast(IntegerType()).alias("dia")
    )

if "genres" in df_series.columns:
    df_series = df_series.withColumn("genre", explode(col("genres"))).select(
                    col("genre.id").cast(IntegerType()).alias("id_genre"),
                    col("genre.name").cast(StringType()).alias("nome_genre"),
                    F.lit(None).cast(IntegerType()).alias("ano"),
                    F.lit(None).cast(IntegerType()).alias("mes"),
                    F.lit(None).cast(IntegerType()).alias("dia")
    )

# Unir as tabelas de gêneros de filmes e séries
df_joined = df_filmes.union(df_series).dropDuplicates(["id_genre"])


# Adicionar a data de execução do Job nas colunas de partição
df_final = df_joined \
    .withColumn("ano", F.lit(ano_exec).cast(IntegerType())) \
    .withColumn("mes", F.lit(mes_exec).cast(IntegerType())) \
    .withColumn("dia", F.lit(dia_exec).cast(IntegerType()))





# Salvar os dados em formato Parquet na camada trusted
output_path = f"s3://{bucket_name}/Trusted/TMDB/Parquet/Genres/"
df_final.write.mode('overwrite').partitionBy("ano", "mes", "dia").option('compression', 'snappy').parquet(output_path)

print(f"Salvo com sucesso em {output_path}")

job.commit()

