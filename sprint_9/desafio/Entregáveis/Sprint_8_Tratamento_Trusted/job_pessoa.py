import sys
from math import ceil
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, explode, lit, coalesce, when
from pyspark.sql import functions as F
import boto3
import re
from datetime import datetime

# Parâmetro do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializar o Spark e Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')
nome_bucket = "desafio-final.data-lake"

# 🔹 Função para encontrar a partição mais recente em um diretório
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


# Ler os dados da RAW Zone
caminho_entrada_s3_pessoas = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/pessoas/")
df_pessoas = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_pessoas)

# Definição de data
ano, mes, dia = caminho_entrada_s3_pessoas.split("/")[-4], caminho_entrada_s3_pessoas.split("/")[-3], caminho_entrada_s3_pessoas.split("/")[-2]



# Renomear e tratar colunas
df_pessoa = df_pessoas.select(
    F.coalesce(F.col("id"), F.lit(0)).cast(IntegerType()).alias("id_pessoa"),
    F.coalesce(F.col("name"), F.lit("N/A")).cast(StringType()).alias("nm_pessoa"),
    F.coalesce(F.col("popularity"), F.lit(0.0)).cast(DoubleType()).alias("vl_popularidade_pessoa"),
    F.coalesce(F.col("gender"), F.lit(0)).cast(IntegerType()).alias("tp_sexo_pessoa"),
    F.coalesce(F.col("place_of_birth"), F.lit("N/A")).cast(StringType()).alias("nm_local_nascimento"),
    F.coalesce(F.col("birthday"), F.lit("1700-01-01")).cast(DateType()).alias("dt_nascimento"),
    F.coalesce(F.col("deathday"), F.lit("1700-01-01")).cast(DateType()).alias("dt_falecimento"),
)

#  **Correção pontual** para evitar erro de data inválida no Spark
df_pessoa = df_pessoa.withColumn(
    "dt_falecimento",
    when((col("id_pessoa") == 3124815) & (col("dt_falecimento") == "1012-08-22"), lit("2012-08-22"))
    .otherwise(col("dt_falecimento"))
)

# Mapear valores de gênero
df_pessoa = df_pessoa.withColumn(
    "tp_sexo_pessoa",
    when(col("tp_sexo_pessoa") == 0, lit("Não especificado"))
    .when(col("tp_sexo_pessoa") == 1, lit("Feminino"))
    .when(col("tp_sexo_pessoa") == 2, lit("Masculino"))
    .when(col("tp_sexo_pessoa") == 3, lit("Não-binário"))
    .otherwise(lit("Não especificado"))
)

df_pessoa = df_pessoa.withColumn("nr_ano", lit(ano))
df_pessoa = df_pessoa.withColumn("nr_mes", lit(mes))
df_pessoa = df_pessoa.withColumn("nr_dia", lit(dia))

df_pessoa.show(5)
df_pessoa.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_pessoa.count()
qtd_arquivos = max(1, ceil(total_registros / 200))

# Salvar os dados em formato Parquet na camada trusted
caminho_saida_s3_pessoa = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_pessoa/"
df_pessoa.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3_pessoa)

print(f"Dados salvos com sucesso em {caminho_saida_s3_pessoa}")

job.commit()
