import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Ler os dados da RAW Zone
bucket_name = "desafio-final.data-lake"
frame_raw = glueContext.create_dynamic_frame.from_catalog(
    database = "desafio.final-raw.tmdb.json.data",
    table_name = "series",
    transformation_ctx="input_data"
)


# Renomear as colunas
colunas = {
    
      "name": "nome",
      "vote_average": "media_votos",
      "vote_count": "quantidade_votos",
      "original_name": "nome_original",
      "overview": "sinopse",
      "first_air_date": "primeira_transmissao",
      "adult": "adulto",
      "backdrop_path": "caminho_fundo",
      "genre_ids": "ids_generos",
      "id": "id",
      "origin_country": "pais_origem",
      "original_language": "idioma_original",
      "original_name": "nome_original",
      "overview": "sinopse",
      "popularity": "popularidade",
      "poster_path": "caminho_poster",
      "partition_0": "ano",
      "partition_1": "mes",
      "partition_2": "dia",

      

}


for old_name, new_name in colunas.items():
    frame_raw = frame_raw.rename_field(old_name, new_name)


# Tratar os dados
frame_raw_tratado = frame_raw.resolveChoice(specs=[
    ('ano', 'cast:int'),
    ('mes', 'cast:int'),
    ('dia', 'cast:int'),
    ])


# Converter DynamicFrame para Spark DataFrame
df_raw = frame_raw_tratado.toDF()


# Salvar os dados em formato Parquet na camada processada do Data Lake
output_path = f"s3://{bucket_name}/Trusted/TMDB/Parquet/Series/"
df_raw.write.mode('overwrite').partitionBy("ano", "mes", "dia").option('compression', 'snappy').parquet(output_path)


job.commit()