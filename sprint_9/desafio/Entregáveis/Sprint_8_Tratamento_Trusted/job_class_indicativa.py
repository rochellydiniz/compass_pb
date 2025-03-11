import sys
import boto3
import re
import builtins
from math import ceil
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, explode, lit, coalesce, when, collect_list, concat_ws, row_number, desc, regexp_extract
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
caminho_entrada_s3_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/class_indicativa_filmes/")
caminho_entrada_s3_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/class_indicativa_series/")


# Ler os dados da RAW Zone
df_classi_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_classi_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

# Definição de data
ano, mes, dia = caminho_entrada_s3_filmes.split("/")[-4], caminho_entrada_s3_filmes.split("/")[-3], caminho_entrada_s3_filmes.split("/")[-2]

# Tratamento de filmes
df_classi_filmes = df_classi_filmes.withColumn("classificacao", explode(col("classificacao"))) \
    .withColumn("release_dates", explode(col("classificacao.release_dates"))) \
    .select(
        F.coalesce(col("movie_id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
        lit("F").alias("tp_titulo"),
        F.coalesce(col("classificacao.iso_3166_1"), lit("")).cast(StringType()).alias("sg_pais_class_indicativa"),
        F.coalesce(col("release_dates.certification"), lit("")).cast(StringType()).alias("cd_class_indicativa"),
    )

# Tratamento de séries
df_classi_series = df_classi_series.withColumn("classificacao", explode(col("classificacao"))) \
    .select(
        F.coalesce(col("serie_id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
        lit("S").alias("tp_titulo"),
        F.coalesce(col("classificacao.iso_3166_1"), lit("")).cast(StringType()).alias("sg_pais_class_indicativa"),
        F.coalesce(col("classificacao.rating"), lit("")).cast(StringType()).alias("cd_class_indicativa"),
    )

# União dos DataFrames
df_class_indicativa_full = df_classi_filmes.unionByName(df_classi_series)

# Filtrar registros válidos
df_class_indicativa_full = df_class_indicativa_full.filter(
    col("cd_class_indicativa").isNotNull() & (col("cd_class_indicativa") != "")
)

# Aplicar regras de classificação
df_class_indicativa_full = df_class_indicativa_full.withColumn(
    "idade_inicial_permitida",

    # 1. Classificações numéricas diretas (ex: 12, 16, 18)
    when(col("cd_class_indicativa").rlike(r"^\d+$"), col("cd_class_indicativa").cast("int"))

    # 2. Classificações numéricas com letras (ex: "14A", "R18+", "K16", "B15")
    .when(col("cd_class_indicativa").rlike(r".*\d+.*"), regexp_extract(col("cd_class_indicativa"), r"(\d+)", 1).cast("int"))

    # 3. Classificações "livres para todos" (ex: "G", "TP", "T", "KN", "AA", "A", "U", "L")
    .when(col("cd_class_indicativa").rlike(r"^(G|TP|T|KN|AA|A|U|L)$"), 0)

    # 4. Classificações "supervisão dos pais recomendada" (ex: "PG", "SPG", "B", "AP")
    .when(col("cd_class_indicativa").rlike(r"^(PG|SPG|B|AP)$"), 10)

    # 5. Classificações para adolescentes (ex: "PG13", "D", "TV-14", "UA")
    .when(col("cd_class_indicativa").rlike(r"^(PG13|D|TV-14|UA)$"), 13)

    # 6. Classificações para adolescentes mais velhos (ex: "14A", "M", "TV-MA")
    .when(col("cd_class_indicativa").rlike(r"^(14A|M|TV-MA)$"), 15)

    # 7. Classificações para adultos (ex: "R", "NC17", "R18+", "TV-MA")
    .when(col("cd_class_indicativa").rlike(r"^(R|NC17|R18+|TV-MA)$"), 17)

    # 8. Classificações desconhecidas ou indefinidas
    .otherwise(None)
)
df_class_indicativa_full = df_class_indicativa_full.distinct()

# Agregação
df_class_indicativa = df_class_indicativa_full.groupBy("sg_pais_class_indicativa", "cd_class_indicativa").agg(
    F.max("idade_inicial_permitida").alias("nr_idade_classificacao")
)

# Adicionar data
df_class_indicativa = df_class_indicativa \
    .withColumn("nr_ano", lit(ano)) \
    .withColumn("nr_mes", lit(mes)) \
    .withColumn("nr_dia", lit(dia))

# Contar registros
total_registros = df_class_indicativa.count()
qtd_arquivos = builtins.max(1, ceil(total_registros / 200))

# Salvar DataFrame em formato Parquet
caminho_saida_s3_classi = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_class_indicativa/"
df_class_indicativa.coalesce(qtd_arquivos).write.mode("overwrite").partitionBy("nr_ano", "nr_mes", "nr_dia").option("compression", "snappy").parquet(caminho_saida_s3_classi)

# Criar uma janela para ordenar dentro de cada id_titulo e tp_titulo
window_spec = Window.partitionBy("id_titulo", "tp_titulo").orderBy(desc("idade_inicial_permitida"))

# Criar o novo DataFrame com a numeração da classificação indicativa iniciando em 1

df_titulo_class = df_class_indicativa_full.withColumn("nr_ordem_class_indicativa", row_number().over(window_spec))  

df_titulo_class = df_titulo_class.select(
    "id_titulo",
    "tp_titulo",
    "sg_pais_class_indicativa",
    "cd_class_indicativa",
    "nr_ordem_class_indicativa"
)
   
# Adicionar data
df_titulo_class = df_titulo_class \
    .withColumn("nr_ano", lit(ano)) \
    .withColumn("nr_mes", lit(mes)) \
    .withColumn("nr_dia", lit(dia))


# Contar registros
total_registros = df_titulo_class.count()
qtd_arquivos = builtins.max(1, ceil(total_registros / 200))

# Salvar segundo DataFrame
caminho_saida_s3_classi = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_assoc_class_indicativa_titulo/"
df_titulo_class.coalesce(qtd_arquivos).write.mode("overwrite").partitionBy("nr_ano", "nr_mes", "nr_dia").option("compression", "snappy").parquet(caminho_saida_s3_classi)

job.commit()
