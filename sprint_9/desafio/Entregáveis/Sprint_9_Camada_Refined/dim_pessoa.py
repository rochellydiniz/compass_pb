import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, year
from awsglue.dynamicframe import DynamicFrame

## Captura os parâmetros do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Inicializa os contextos do Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar tabela de pessoas da camada Trusted
base_dados = "desafiofinal_trusted_data"
trusted_tb_pessoa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_pessoa").toDF()

# Ajustar as colunas de falecimento, considerando que '1700-01-01' representa nulo
dim_pessoa = trusted_tb_pessoa \
    .withColumn("dt_falecimento", when(col("dt_falecimento") == "1700-01-01", None).otherwise(col("dt_falecimento"))) \
    .withColumn("fl_falecido", when(col("dt_falecimento").isNotNull(), 1).otherwise(0)) \
    .withColumn("fl_falecido_crianca", 
                when((col("dt_falecimento").isNotNull()) & 
                     (year(col("dt_falecimento")) - year(col("dt_nascimento")) < 13), 1)
                .otherwise(0)) \
    .select(
        "id_pessoa", "nm_pessoa", "tp_sexo_pessoa", "vl_popularidade_pessoa", "nm_local_nascimento",
        "dt_nascimento", "dt_falecimento", "fl_falecido", "fl_falecido_crianca"
    )

# Converter para DynamicFrame para escrita no S3
dim_pessoa_dyf = DynamicFrame.fromDF(dim_pessoa, glueContext, "dim_pessoa")

# Escrever a Dimensão Pessoa na camada Refined no formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=dim_pessoa_dyf, 
    connection_type="s3", 
    connection_options={"path": "s3://desafio-final.data-lake/Refined/dim_pessoa/"},
    format="parquet"
)

# Finalizar o Job no Glue
job.commit()
