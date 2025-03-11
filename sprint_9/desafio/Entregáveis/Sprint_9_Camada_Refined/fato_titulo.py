import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

## Captura os parâmetros do Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

## Inicializa os contextos do Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar tabelas da camada Trusted
base_dados = "desafiofinal_trusted_data"

trusted_tb_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_titulo").toDF()
trusted_tb_assoc_keyword_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_assoc_keyword_titulo").toDF()
trusted_tb_elenco = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_elenco").toDF()
trusted_tb_pessoa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_pessoa").toDF()
trusted_tb_assoc_class_indicativa_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_assoc_class_indicativa_titulo").toDF()
trusted_tb_class_indicativa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_class_indicativa").toDF()

# Criando os joins necessários para construir a fato_titulo
df_fato_titulo = trusted_tb_titulo.alias("t") \
    .join(trusted_tb_assoc_keyword_titulo.alias("k"), 
          (F.col("t.id_titulo") == F.col("k.id_titulo")) & (F.col("t.tp_titulo") == F.col("k.tp_titulo")), "left") \
    .join(trusted_tb_elenco.alias("e"), 
          (F.col("t.id_titulo") == F.col("e.id_titulo")) & (F.col("t.tp_titulo") == F.col("e.tp_titulo")), "left") \
    .join(trusted_tb_pessoa.alias("p"), F.col("e.id_pessoa") == F.col("p.id_pessoa"), "left") \
    .join(trusted_tb_assoc_class_indicativa_titulo.alias("a"), 
          (F.col("t.id_titulo") == F.col("a.id_titulo")) & (F.col("t.tp_titulo") == F.col("a.tp_titulo")), "left") \
    .join(trusted_tb_class_indicativa.alias("c"), 
          (F.col("a.sg_pais_class_indicativa") == F.col("c.sg_pais_class_indicativa")) &
          (F.col("a.cd_class_indicativa") == F.col("c.cd_class_indicativa")),
          "left")

# Primeira agregação SEM id_tempo
df_fato_titulo = df_fato_titulo.groupBy("t.id_titulo", "t.tp_titulo") \
    .agg(
        # Agregar dt_lancamento corretamente (evita ambiguidade)
        F.max(F.col("t.dt_lancamento")).alias("dt_lancamento"),

        # Métricas de popularidade e engajamento
        F.max("t.vl_popularidade_titulo").alias("vl_popularidade_titulo"),
        F.max("t.vl_nota_media").alias("vl_media_nota_titulo"),
        F.max("t.qt_votos").alias("qt_votos"),
        F.countDistinct("k.id_keyword").alias("qt_keywords"),

        # Quantidade de protagonistas
        F.countDistinct("e.id_pessoa").alias("qt_protagonistas"),

        # Mediana da idade dos protagonistas (ignora registros inválidos)
        F.expr(
            """
            percentile_approx(
                CASE 
                    WHEN t.dt_lancamento != '1700-01-01' AND p.dt_nascimento != '1700-01-01' 
                    THEN year(t.dt_lancamento) - year(p.dt_nascimento) 
                    ELSE NULL 
                END, 
                0.5
            )
            """
        ).alias("md_idade_protagonistas"),

        # Diversidade Etária dos Protagonistas
        F.countDistinct(F.when(
            (F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento")) <= 12) & 
            (F.col("p.dt_nascimento") != "1700-01-01") & 
            (F.col("t.dt_lancamento") != "1700-01-01"), 
            F.col("e.id_pessoa")
        )).alias("qt_protagonistas_criancas"),

        F.countDistinct(F.when(
            (F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento")) >= 18) & 
            (F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento")) < 60) & 
            (F.col("p.dt_nascimento") != "1700-01-01") & 
            (F.col("t.dt_lancamento") != "1700-01-01"), 
            F.col("e.id_pessoa")
        )).alias("qt_protagonistas_adultos"),

        F.countDistinct(F.when(
            (F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento")) >= 60) & 
            (F.col("p.dt_nascimento") != "1700-01-01") & 
            (F.col("t.dt_lancamento") != "1700-01-01"), 
            F.col("e.id_pessoa")
        )).alias("qt_protagonistas_idosos"),

        # Classificação Indicativa
        F.min("c.nr_idade_classificacao").alias("nr_idade_min_classificacao"),

        # Flag para indicar se o lançamento foi no fim de semana
        F.when(F.dayofweek(F.max("t.dt_lancamento")) .isin([7, 1]), 1).otherwise(0).alias("fl_lancamento_fim_de_semana")
    )


df_fato_titulo = df_fato_titulo.withColumn(
    "id_tempo",
    F.concat_ws("",
                F.format_string("%04d", F.year(F.col("dt_lancamento"))),
                F.format_string("%02d", F.month(F.col("dt_lancamento"))),
                F.format_string("%02d", F.dayofmonth(F.col("dt_lancamento")))
    ).cast("int")
) 

df_fato_titulo.show(5)

df_fato_titulo = df_fato_titulo.withColumn("ano_lancamento", F.year(col("dt_lancamento")).cast(IntegerType()))

# Reparticionar antes de escrever
df_fato_titulo = df_fato_titulo.repartition("ano_lancamento")

# Converter para DynamicFrame
df_fato_titulo_dyf = DynamicFrame.fromDF(df_fato_titulo, glueContext, "fato_titulo")

# Escrever os dados no S3 particionados por tipo de título e ano
glueContext.write_dynamic_frame.from_options(
    frame=df_fato_titulo_dyf, 
    connection_type="s3", 
    connection_options={
        "path": "s3://desafio-final.data-lake/Refined/fato_titulo/",
        "partitionKeys": ["ano_lancamento"]
    },
    format="parquet"
)


# Finalizar o Job no Glue
job.commit()
