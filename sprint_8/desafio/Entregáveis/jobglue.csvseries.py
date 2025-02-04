import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, regexp_replace



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
    database = "desafio.final-raw.csv.data",
    table_name = "series",
    transformation_ctx="input_data"
)


# Renomear as colunas
colunas = {
    "titulopincipal": "tituloprincipal",
    "partition_0": "ano",
    "partition_1": "mes",
    "partition_2": "dia",
}


for old_name, new_name in colunas.items():
    frame_raw = frame_raw.rename_field(old_name, new_name)


# Tratar os dados
frame_raw_tratado = frame_raw.resolveChoice(specs=[
    ('anolancamento', 'cast:int'),
    ('anotermino', 'cast:int'),
    ('tempominutos', 'cast:int'),
    ('notamedia', 'cast:double'),
    ('numerovotos', 'cast:long'),
    ('anonascimento', 'cast:int'),
    ('anofalecimento', 'cast:int'),
    ('ano', 'cast:int'),
    ('mes', 'cast:int'),
    ('dia', 'cast:int')
    ])


# Converter DynamicFrame para Spark DataFrame
df_raw = frame_raw_tratado.toDF()


# Filtrar títulos com gêneros de interesse
df_filtrado = df_raw.filter(
    (col("genero").contains("Mystery"))
)


# Salvar os dados em formato Parquet na camada processada do Data Lake
output_path = f"s3://{bucket_name}/Trusted/Local/Parquet/Series/"
df_filtrado.write.mode('overwrite').option('compression', 'snappy').parquet(output_path)


job.commit()