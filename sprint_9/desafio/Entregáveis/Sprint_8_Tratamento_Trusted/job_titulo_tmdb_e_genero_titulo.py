import sys
import boto3
import re
from datetime import datetime
from math import ceil
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, coalesce, explode, when, size, element_at, upper
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

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

# Função para encontrar a partição mais recente em um diretório
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

# Definir caminhos de entrada dinamicamente
caminho_entrada_s3_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/filmes/")
caminho_entrada_s3_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/series/")
caminho_entrada_s3_pais = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/pais_origem_filmes/")
caminho_entrada_s3_ids_externos_filmes = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/ids_externos_filmes/")
caminho_entrada_s3_ids_externos_series = obter_ultima_particao(nome_bucket, "RAW Zone/TMDB/JSON/ids_externos_series/")

# Ler os dados da RAW Zone

def carregar_dados(caminho, nome):
    if caminho:
        print(f"Carregando {nome} da partição {caminho}")
        return spark.read.option("multiline", "true").json(caminho)
    else:
        print(f"Nenhum dado encontrado para {nome}. Pulando...")
        return None

df_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_filmes)
df_series = spark.read.option("multiline", "true").json(caminho_entrada_s3_series)
df_pais_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_pais)
df_id_externo_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_ids_externos_filmes)
df_id_externo_series = spark.read.option("multiline", "true").json(caminho_entrada_s3_ids_externos_series)

# Caso nenhum dado seja carregado, interromper o job
if not df_filmes and not df_series:
    print("Nenhum dado carregado. Encerrando job.")
    job.commit()
    sys.exit(0)

# Extração de ano, mês e dia a partir do caminho da partição
ano, mes, dia = caminho_entrada_s3_filmes.split("/")[-4], caminho_entrada_s3_filmes.split("/")[-3], caminho_entrada_s3_filmes.split("/")[-2]


# Renomear colunas Filmes
df_filmes = df_filmes.select(
    F.coalesce(F.col("id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.lit("F").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("title"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    F.col("genre_ids"),
    F.coalesce(F.col("original_title"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    F.upper(F.coalesce(F.col("original_language"), F.lit("N/A"))).cast(StringType()).alias("sg_idioma"),
    F.coalesce(F.col("popularity"), F.lit(0.0)).cast(DoubleType()).alias("vl_popularidade_titulo"),
    F.coalesce(F.col("vote_average"), F.lit(0.0)).cast(DoubleType()).alias("vl_nota_media"),
    F.coalesce(F.col("vote_count"), F.lit(0)).cast(IntegerType()).alias("qt_votos"),
    F.coalesce(F.col("release_date"), F.lit("1700-01-01")).cast(DateType()).alias("dt_lancamento"),
    F.coalesce(F.col("overview"), F.lit("N/A")).cast(StringType()).alias("ds_sinopse"),
    F.lit(ano).cast(StringType()).alias("ano"),
    F.lit(mes).cast(StringType()).alias("mes"),
    F.lit(dia).cast(StringType()).alias("dia")
)

# Tratar valores nulos
df_filmes_tmdb = df_filmes \
    .withColumn("ds_sinopse", 
        F.when(F.col("ds_sinopse").isNull(),"N/A").otherwise(F.col("ds_sinopse"))
    ) \
    .withColumn("ds_sinopse", 
        F.when(F.col("ds_sinopse") == "", "N/A").otherwise(F.col("ds_sinopse"))
    )

# Selecionar apenas o primeiro país da lista
df_series = df_series.withColumn(
    "origin_country", 
    when(size(col("origin_country")) > 0, col("origin_country")[0]).otherwise(lit("N/A"))
)

# Renomear colunas e selecionar os dados
df_series = df_series.select(
    coalesce(col("id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
    lit("S").cast(StringType()).alias("tp_titulo"),
    coalesce(col("name"), lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    col("genre_ids"),
    coalesce(col("original_name"), lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    coalesce(col("origin_country"), lit("N/A")).cast(StringType()).alias("sg_pais_origem"),
    F.upper(F.coalesce(F.col("original_language"), F.lit("N/A"))).cast(StringType()).alias("sg_idioma"),
    coalesce(col("popularity"), lit(0.0)).cast(DoubleType()).alias("vl_popularidade_titulo"),
    coalesce(col("vote_average"), lit(0.0)).cast(DoubleType()).alias("vl_nota_media"),
    coalesce(col("vote_count"), lit(0)).cast(IntegerType()).alias("qt_votos"),
    coalesce(col("first_air_date"), lit("1700-01-01")).cast(DateType()).alias("dt_lancamento"),
    coalesce(col("overview"), lit("N/A")).cast(StringType()).alias("ds_sinopse")
)

# Tratar nulos
df_series_tmdb = df_series \
    .withColumn("ds_sinopse", F.when(F.col("ds_sinopse").isNull(),"N/A").otherwise(F.col("ds_sinopse"))) \
    .withColumn("ds_sinopse", F.when(F.col("ds_sinopse") == "", "N/A").otherwise(F.col("ds_sinopse")))  

# Exluir coluna genre_ids
df_filmes_tmdb = df_filmes_tmdb.drop("genre_ids")
df_series_tmdb = df_series_tmdb.drop("genre_ids")

# Tratar IDs externos
df_id_externo_filmes = df_id_externo_filmes.select(
    F.coalesce(F.col("movie_id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.lit("F").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("imdb_id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
) 

df_id_externo_series = df_id_externo_series.select(
    F.coalesce(F.col("serie_id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.lit("S").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("imdb_id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
) 

# Unir dfs de IDs externos
df_id_externo = df_id_externo_filmes.unionByName(df_id_externo_series)
df_id_externo = df_id_externo_filmes.distinct()

# Tratar país de origem (filmes) - pegar apenas o primeiro país da lista
df_pais_origem_filmes = df_pais_filmes.withColumn(
    "origin_country", 
    coalesce(element_at(col("origin_country"), 1), lit("N/A"))
).select(
    coalesce(col("movie_id"), lit(0)).cast(IntegerType()).alias("id_titulo"),
    col("origin_country").cast(StringType()).alias("sg_pais_origem")
)

# Unir dfs pelo id_titulo
df_filmes_tmdb = df_filmes_tmdb.join(df_pais_origem_filmes, on="id_titulo", how="left")

# Substituir valores nulos
df_filmes_tmdb = df_filmes_tmdb.withColumn("sg_pais_origem", coalesce(col("sg_pais_origem"), lit("N/A")))

# Selecionar apenas as colunas desejadas
df_filmes_tmdb = df_filmes_tmdb.select(
    col("id_titulo"),
    col("tp_titulo"),
    col("nm_titulo_principal"),
    col("sg_pais_origem"), 
    col("nm_titulo_original"),
    col("sg_idioma"),
    col("vl_popularidade_titulo"),
    col("vl_nota_media"),
    col("qt_votos"),
    col("dt_lancamento"),
    col("ds_sinopse")
)

# Unir dataframes de filmes e séries pelo id_titulo
df_titulo_tmdb = df_filmes_tmdb.unionByName(df_series_tmdb, allowMissingColumns=True)

# Unir id_externo com id_titulo
df_titulo_tmdb = df_titulo_tmdb.join(df_id_externo, on=["id_titulo", "tp_titulo"], how="left").withColumn("id_imdb", coalesce(col("id_imdb"), lit("N/A")))  

df_titulo_tmdb = df_titulo_tmdb \
    .withColumn("nr_ano", lit(ano).cast(StringType())) \
    .withColumn("nr_mes", lit(mes).cast(StringType())) \
    .withColumn("nr_dia", lit(dia).cast(StringType()))


df_titulo_tmdb.show(5)
df_titulo_tmdb.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros = df_titulo_tmdb.count()
qtd_arquivos = max(1, ceil(total_registros / 200))


# Salvar a df titulo
caminho_saida_s3 = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_titulo_tmdb/"
df_titulo_tmdb.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3)

################################################################################
# Criar a df de associação genero_titulo devido (N:N)
df_genero_filmes = df_filmes.select(
    col("id_titulo"),
    col("tp_titulo"),
    F.explode(col("genre_ids")).alias("id_genero")
)

df_genero_series = df_series.select(
    col("id_titulo"),
    col("tp_titulo"),
    F.explode(col("genre_ids")).alias("id_genero")
)

# Unir filmes e séries
df_join_genero_titulo = df_genero_filmes.unionByName(df_genero_series)
df_join_genero_titulo = df_join_genero_titulo.distinct()

df_join_genero_titulo = df_join_genero_titulo.withColumn("nr_ano", lit(ano))
df_join_genero_titulo = df_join_genero_titulo.withColumn("nr_mes", lit(mes))
df_join_genero_titulo = df_join_genero_titulo.withColumn("nr_dia", lit(dia))

df_join_genero_titulo.show(5)
df_join_genero_titulo.printSchema()

# Contar total de registros e definir quantidade de registros por arquivo = 200
total_registros_gt = df_join_genero_titulo.count()
qtd_arquivos = max(1, ceil(total_registros_gt / 200))

# Salvar a df genero_titulo
caminho_saida_s3_gt = "s3://desafio-final.data-lake/Trusted/TMDB/Parquet/tb_assoc_genero_titulo/"
df_join_genero_titulo.repartition(qtd_arquivos).write.mode('overwrite').partitionBy("nr_ano", "nr_mes", "nr_dia").option('compression', 'snappy').parquet(caminho_saida_s3_gt)

# Finalizar job
job.commit()
