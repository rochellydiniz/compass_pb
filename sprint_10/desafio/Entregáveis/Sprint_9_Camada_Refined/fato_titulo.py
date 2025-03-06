import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
import pyspark.sql.window as W
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Carregar os dados da Trusted
base_dados = "desafiofinal_trusted_data"

trusted_tb_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_titulo").toDF()

trusted_tb_assoc_genero_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_assoc_genero_titulo").toDF()
trusted_tb_assoc_keyword_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_assoc_keyword_titulo").toDF()
trusted_tb_elenco = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_elenco").toDF()
trusted_tb_pessoa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_pessoa").toDF()
trusted_tb_assoc_class_indicativa_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_assoc_class_indicativa_titulo").toDF()
trusted_tb_class_indicativa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_class_indicativa").toDF()

# Criando os joins
df_fato_titulo = trusted_tb_titulo.alias("t") \
    .join(trusted_tb_assoc_genero_titulo.alias("g"), F.col("t.id_titulo") == F.col("g.id_titulo"), "left") \
    .join(trusted_tb_assoc_keyword_titulo.alias("k"), F.col("t.id_titulo") == F.col("k.id_titulo"), "left") \
    .join(trusted_tb_elenco.alias("e"), F.col("t.id_titulo") == F.col("e.id_titulo"), "left") \
    .join(trusted_tb_pessoa.alias("p"), F.col("e.id_pessoa") == F.col("p.id_pessoa"), "left") \
    .join(trusted_tb_assoc_class_indicativa_titulo.alias("a"), F.col("t.id_titulo") == F.col("a.id_titulo"), "left") \
    .join(trusted_tb_class_indicativa.alias("c"), 
          (F.col("a.sg_pais_class_indicativa") == F.col("c.sg_pais_class_indicativa")) &
          (F.col("a.cd_class_indicativa") == F.col("c.cd_class_indicativa")),
          "left")

# Criar colunas calculadas
df_fato_titulo = df_fato_titulo.withColumn("id_tempo", 
                   F.concat_ws("", 
                               F.format_string("%04d", F.year(F.col("t.dt_lancamento"))),
                               F.format_string("%02d", F.month(F.col("t.dt_lancamento"))),
                               F.format_string("%02d", F.dayofmonth(F.col("t.dt_lancamento")))))

df_fato_titulo = df_fato_titulo.groupBy("t.id_titulo", "t.tp_titulo") \
    .agg(
        F.max("id_tempo").alias("id_tempo"),
        F.max("t.vl_popularidade_titulo").alias("vl_popularidade_titulo"),
        F.max("t.vl_nota_media").alias("vl_nota_media"),
        F.max("t.qt_votos").alias("qt_votos"),
        F.countDistinct("g.id_genero").alias("qt_generos"),
        F.countDistinct("k.id_keyword").alias("qt_keywords"),
        F.countDistinct(F.when((F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento"))) <= 12, F.col("e.id_pessoa"))).alias("qt_criancas_no_elenco"),
        F.expr("percentile_approx(year(t.dt_lancamento) - year(p.dt_nascimento), 0.5)").alias("md_idade_criancas"),
        F.when(
            F.countDistinct(F.when((F.year(F.col("t.dt_lancamento")) - F.year(F.col("p.dt_nascimento"))) <= 12, F.col("e.id_pessoa"))) > 0, 1
        ).otherwise(0).alias("fl_tem_crianca"),
        F.min("c.nr_idade_classificacao").alias("nr_idade_min_classificacao")
    )

# Convertendo para DynamicFrame
df_fato_titulo = DynamicFrame.fromDF(df_fato_titulo, glueContext, "fato_titulo")

# Gravando na camada Refined
glueContext.write_dynamic_frame.from_options(frame=df_fato_titulo, connection_type="s3", connection_options={"path": "s3://desafio-final.data-lake/Refined/fato_titulo/"}, format="parquet")

job.commit()