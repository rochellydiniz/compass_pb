import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Criar sequência de datas de 1950-01-01 até 2030-12-31
data_inicio = datetime.strptime("1950-01-01", "%Y-%m-%d")
data_fim = datetime.strptime("2029-12-31", "%Y-%m-%d")
dias = (data_fim - data_inicio).days

df_dim_tempo = spark.range(0, dias + 1).select(
    F.expr(f"date_add(to_date('{data_inicio.strftime('%Y-%m-%d')}'), CAST(id AS INT))").alias("dt")
)

# Criar colunas calculadas
df_dim_tempo = df_dim_tempo.withColumn(
    "id_tempo", 
    F.concat_ws("", 
        F.format_string("%04d", F.year(F.col("dt"))),
        F.format_string("%02d", F.month(F.col("dt"))),
        F.format_string("%02d", F.dayofmonth(F.col("dt")))
    )
).withColumn("nr_ano", F.year(F.col("dt"))
).withColumn("nr_mes", F.month(F.col("dt"))
).withColumn("nr_dia", F.dayofmonth(F.col("dt"))
).withColumn("nr_trimestre", F.ceil(F.month(F.col("dt")) / 3.0)
).withColumn("nr_bimestre", F.ceil(F.month(F.col("dt")) / 2.0)
).withColumn("fl_final_de_semana", 
    F.when(F.dayofweek(F.col("dt")).isin([1, 7]), 1).otherwise(0)
)

df_dim_tempo = df_dim_tempo.withColumn("nr_decada", (F.year(F.col("dt")) / 10).cast("int") * 10)


# Adicionar nome do mês e nome do dia da semana de forma otimizada
df_dim_tempo = df_dim_tempo.withColumn(
    "nm_mes",
    F.when(F.month(F.col("dt")) == 1, "Janeiro")
     .when(F.month(F.col("dt")) == 2, "Fevereiro")
     .when(F.month(F.col("dt")) == 3, "Março")
     .when(F.month(F.col("dt")) == 4, "Abril")
     .when(F.month(F.col("dt")) == 5, "Maio")
     .when(F.month(F.col("dt")) == 6, "Junho")
     .when(F.month(F.col("dt")) == 7, "Julho")
     .when(F.month(F.col("dt")) == 8, "Agosto")
     .when(F.month(F.col("dt")) == 9, "Setembro")
     .when(F.month(F.col("dt")) == 10, "Outubro")
     .when(F.month(F.col("dt")) == 11, "Novembro")
     .otherwise("Dezembro")
)

df_dim_tempo = df_dim_tempo.withColumn(
    "nm_dia_semana",
    F.when(F.dayofweek(F.col("dt")) == 1, "Domingo")
     .when(F.dayofweek(F.col("dt")) == 2, "Segunda-feira")
     .when(F.dayofweek(F.col("dt")) == 3, "Terça-feira")
     .when(F.dayofweek(F.col("dt")) == 4, "Quarta-feira")
     .when(F.dayofweek(F.col("dt")) == 5, "Quinta-feira")
     .when(F.dayofweek(F.col("dt")) == 6, "Sexta-feira")
     .otherwise("Sábado")
)

df_dim_tempo = df_dim_tempo.repartition(5, "nr_decada")


# Converter para DynamicFrame para salvar no S3
dynamic_frame = DynamicFrame.fromDF(df_dim_tempo, glueContext, "dim_tempo")

# Gravar na camada Refined (S3) com particionamento
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://desafio-final.data-lake/Refined/dim_tempo/", "partitionKeys": ["nr_decada"]},
    format="parquet"
)

job.commit()
