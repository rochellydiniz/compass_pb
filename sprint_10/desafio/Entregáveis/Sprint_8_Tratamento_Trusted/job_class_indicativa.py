import sys
from math import ceil
import builtins
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, explode, max, lit, row_number, desc, regexp_extract, when
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

# Definir caminho dos arquivos S3
caminho_entrada_s3_filmes = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/class_indicativa_filmes/2025/02/14/"
caminho_entrada_s3_series = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/class_indicativa_series/2025/02/14/"

# Ler os dados da RAW Zone
df_classi_filmes = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_filmes)
df_classi_series = spark.read.format("json").option("multiline", "true").load(caminho_entrada_s3_series)

# Definição de data
ano, mes, dia = "2025", "02", "14"

# Tratamento de filmes
df_classi_filmes = df_classi_filmes.withColumn("classificacao", explode("classificacao")) \
    .withColumn("release_dates", explode("classificacao.release_dates")) \
    .select(
        F.coalesce(col("movie_id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
        lit("F").alias("tp_titulo"),
        F.coalesce(col("classificacao.iso_3166_1"), lit(""))
        .cast(StringType()).alias("sg_pais_class_indicativa"),
        F.coalesce(col("release_dates.certification"), lit(""))
        .cast(StringType()).alias("cd_class_indicativa"),
    )

# Tratamento de séries
df_classi_series = df_classi_series.withColumn("classificacao", explode("classificacao")) \
    .select(
        F.coalesce(col("serie_id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
        lit("S").alias("tp_titulo"),
        F.coalesce(col("classificacao.iso_3166_1"), lit(""))
        .cast(StringType()).alias("sg_pais_class_indicativa"),
        F.coalesce(col("classificacao.rating"), lit(""))
        .cast(StringType()).alias("cd_class_indicativa"),
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

# Agregação
df_class_indicativa = df_class_indicativa_full.groupBy("sg_pais_class_indicativa", "cd_class_indicativa").agg(
    max("idade_inicial_permitida").alias("nr_idade_classificacao")
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
df_titulo_class = df_class_indicativa_full.withColumn("nr_ordem_class_indicativa", row_number().over(window_spec))  # Reinicia para cada (id_titulo, tp_titulo)

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
