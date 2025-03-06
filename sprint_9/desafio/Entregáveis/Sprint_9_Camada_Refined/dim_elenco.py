import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Nome do banco
base_dados = "desafio.final-trusted.data"

# Carregar os dados da camada Trusted
trusted_tb_elenco = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_elenco").toDF()
trusted_tb_pessoa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_pessoa").toDF()
trusted_tb_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_titulo").toDF()

# Criar os JOINs
df_dim_elenco = trusted_tb_elenco.alias("e") \
    .join(trusted_tb_pessoa.alias("p"), F.col("e.id_pessoa") == F.col("p.id_pessoa"), "inner") \
    .join(trusted_tb_titulo.alias("t"), F.col("e.id_titulo") == F.col("t.id_titulo"), "inner")

# Criar colunas calculadas
df_dim_elenco = df_dim_elenco.withColumn(
    "vl_idade_lancamento",
    F.when(
        (F.col("t.dt_lancamento").isNotNull()) & (F.col("p.dt_nascimento").isNotNull()),
        F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento"))
    ).otherwise(-1)
).withColumn(
    "fl_crianca_lancamento", F.when(F.col("vl_idade_lancamento") <= 12, 1).otherwise(0)
)

# Selecionar colunas finais
df_dim_elenco = df_dim_elenco.select(
    "e.id_titulo",
    "e.tp_titulo",
    "e.id_pessoa",
    "vl_idade_lancamento",
    "fl_crianca_lancamento"
)

# Converter para DynamicFrame para salvar no S3
dynamic_frame = DynamicFrame.fromDF(df_dim_elenco, glueContext, "dim_elenco")

# Gravar na camada Refined (S3)
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://desafio-final.data-lake/Refined/dim_elenco/"},
    format="parquet"
)

job.commit()