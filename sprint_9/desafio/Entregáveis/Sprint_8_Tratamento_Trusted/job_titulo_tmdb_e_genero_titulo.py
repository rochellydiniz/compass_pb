import sys
from math import ceil
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, coalesce, explode, when
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
caminho_entrada_s3_filmes = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/filmes/2025/02/08/"
caminho_entrada_s3_series = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/series/2025/02/08/"
caminho_entrada_s3_pais = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/pais_origem_filmes/2025/02/15/"
caminho_entrada_s3_ids_externos_filmes = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/ids_externos_filmes/2025/02/16/"
caminho_entrada_s3_ids_externos_series = "s3://desafio-final.data-lake/RAW Zone/TMDB/JSON/ids_externos_series/2025/02/16/"

df_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_filmes)
df_series = spark.read.option("multiline", "true").json(caminho_entrada_s3_series)
df_pais_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_pais)
df_id_externo_filmes = spark.read.option("multiline", "true").json(caminho_entrada_s3_ids_externos_filmes)
df_id_externo_series = spark.read.option("multiline", "true").json(caminho_entrada_s3_ids_externos_series)

ano = "2025"
mes = "02"
dia = "08"


# Renomear colunas Filmes
df_filmes = df_filmes.select(
    F.coalesce(F.col("id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.lit("F").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("title"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    F.col("genre_ids"),
    F.lit("").cast(StringType()).alias("sg_pais_origem"),
    F.coalesce(F.col("original_title"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    F.coalesce(F.col("original_language"), F.lit("N/A")).cast(StringType()).alias("sg_idioma"),
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
    .withColumn("dt_lancamento", 
        F.when(F.col("dt_lancamento").isNull(), lit("1700-01-01").cast(DateType())).otherwise(F.col("dt_lancamento"))
    ) \
    .withColumn("ds_sinopse", 
        F.when(F.col("ds_sinopse").isNull(),"N/A").otherwise(F.col("ds_sinopse"))
    ) \
    .withColumn("ds_sinopse", 
        F.when(F.col("ds_sinopse") == "", "N/A").otherwise(F.col("ds_sinopse"))
    )

# Renomear colunas series e explodir "origin_country"
df_series = df_series.withColumn("origin_country", explode("origin_country")).select(
    F.coalesce(F.col("id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.lit("S").cast(StringType()).alias("tp_titulo"),
    F.coalesce(F.col("name"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_principal"),
    F.col("genre_ids"),
    F.coalesce(F.col("original_name"), F.lit("N/A")).cast(StringType()).alias("nm_titulo_original"),
    F.coalesce(F.col("origin_country"), F.lit("N/A")).cast(StringType()).alias("sg_pais_origem"),
    F.coalesce(F.col("original_language"), F.lit("N/A")).cast(StringType()).alias("sg_idioma"),
    F.coalesce(F.col("popularity"), F.lit(0.0)).cast(DoubleType()).alias("vl_popularidade_titulo"),
    F.coalesce(F.col("vote_average"), F.lit(0.0)).cast(DoubleType()).alias("vl_nota_media"),
    F.coalesce(F.col("vote_count"), F.lit(0)).cast(IntegerType()).alias("qt_votos"),
    F.coalesce(F.col("first_air_date"), F.lit("1700-01-01")).cast(DateType()).alias("dt_lancamento"),
    F.coalesce(F.col("overview"), F.lit("N/A")).cast(StringType()).alias("ds_sinopse")
)

# Tratar nulos
df_series_tmdb = df_series \
    .withColumn("dt_lancamento", F.when(F.col("dt_lancamento").isNull(),"1700-01-01").otherwise(F.col("dt_lancamento"))) \
    .withColumn("ds_sinopse", F.when(F.col("ds_sinopse").isNull(),"N/A").otherwise(F.col("ds_sinopse"))) \
    .withColumn("ds_sinopse", F.when(F.col("ds_sinopse") == "", "N/A").otherwise(F.col("ds_sinopse")))  

# Exluir coluna genre_ids
df_filmes_tmdb = df_filmes_tmdb.drop("genre_ids")
df_series_tmdb = df_series_tmdb.drop("genre_ids")

# Tratar IDs externos
df_id_externo_filmes = df_id_externo_filmes.select(
    F.coalesce(F.col("movie_id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.coalesce(F.col("imdb_id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
)

df_id_externo_series = df_id_externo_series.select(
    F.coalesce(F.col("movie_id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.coalesce(F.col("imdb_id"), F.lit("N/A")).cast(StringType()).alias("id_imdb"),
)

# Unir dfs de IDs externos
df_id_externo = df_id_externo_filmes.unionByName(df_id_externo_series)

# Tratar país de origem (filmes)
df_pais_origem_filmes = df_pais_filmes.withColumn("origin_country", explode(col("origin_country"))).select(
    F.coalesce(col("movie_id"), F.lit(0)).cast(IntegerType()).alias("id_titulo"),
    F.coalesce(col("origin_country"), F.lit("N/A")).cast(StringType()).alias("sg_pais_origem"),
)

# Remover qualquer coluna sg_pais_origem que já exista em df_filmes_tmdb antes do JOIN
if "sg_pais_origem" in df_filmes_tmdb.columns:
    df_filmes_tmdb = df_filmes_tmdb.drop("sg_pais_origem")

# Unir dfs pelo id_titulo
df_filmes_tmdb = df_filmes_tmdb.join(df_pais_origem_filmes, on="id_titulo", how="left")

# Substituir valores nulos
df_filmes_tmdb = df_filmes_tmdb.withColumn("sg_pais_origem", coalesce(col("sg_pais_origem"), lit("N/A")))

# Remover colunas duplicadas
df_filmes_tmdb = df_filmes_tmdb.drop("sg_pais_origem_temp")

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
df_titulo_tmdb = df_titulo_tmdb.join(df_id_externo, on="id_titulo", how="left").withColumn("id_imdb", coalesce(col("id_imdb"), lit("N/A")))

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
