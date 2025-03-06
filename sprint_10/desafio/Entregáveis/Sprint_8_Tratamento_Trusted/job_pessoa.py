import sys
from math import ceil
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

# Parâmetro do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializar o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Ler os dados da RAW Zone
caminho_entrada_s3_pessoas = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/pessoas/2025/02/14/"

df_pessoas = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_pessoas)

ano = "2025"
mes = "02"
dia = "14"

# Renomear e tratar colunas
df_pessoas = df_pessoas.select(
    F.coalesce(F.col("id"), F.lit(0)).cast(IntegerType()).alias("id_pessoa"),
    F.coalesce(F.col("name"), F.lit("N/A")).cast(StringType()).alias("nm_pessoa"),
    F.coalesce(F.col("popularity"), F.lit(0.0)).cast(DoubleType()).alias("vl_popularidade_pessoa"),
    F.coalesce(F.col("gender"), F.lit(0)).cast(IntegerType()).alias("tp_sexo_pessoa"),
    F.coalesce(F.col("place_of_birth"), F.lit("N/A")).cast(StringType()).alias("nm_local_nascimento"),
    F.coalesce(F.col("birthday"), F.lit("1700-01-01")).cast(DateType()).alias("dt_nascimento"),
    F.coalesce(F.col("deathday"), F.lit("1700-01-01")).cast(DateType()).alias("dt_falecimento"),
)

# Mapear valores de genero
df_pessoas = df_pessoas.withColumn(
    "tp_sexo_pessoa",
    when(col("tp_sexo_pessoa") == 0, lit("Não especificado"))
    .when(col("tp_sexo_pessoa") == 1, lit("Feminino"))
    .when(col("tp_sexo_pessoa") == 2, lit("Masculino"))
    .when(col("tp_sexo_pessoa") == 3, lit("Não-binário"))
    .otherwise(lit("Não especificado"))
)

df_pessoas = df_pessoas.withColumn("ano", lit(ano))
df_pessoas = df_pessoas.withColumn("mes", lit(mes))
df_pessoas = df_pessoas.withColumn("dia", lit(dia))

df_pessoas.show(5)
df_pessoas.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_pessoas.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3_pessoas = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/Pessoas/"
df_pessoas.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("ano", "mes", "dia").option('compression', 'snappy').parquet(caminho_saida_s3_pessoas)

print(f"Dados salvos com sucesso em {caminho_saida_s3_pessoas}")

job.commit()
