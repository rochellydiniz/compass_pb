import sys
from math import ceil
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, lit, coalesce, when
from pyspark.sql.types import StringType, IntegerType

# Parâmetro do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializar o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler dados da RAW Zone (Glue Catalog)
caminho_entrada_s3_filmes = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/creditos_filmes/2025/02/12/"
caminho_entrada_s3_series = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/creditos_series/2025/02/12/"

df_creditos_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_creditos_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

ano = "2025"
mes = "02"
dia = "12"


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