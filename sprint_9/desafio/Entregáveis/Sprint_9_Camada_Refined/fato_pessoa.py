import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, avg, year, month, dayofmonth, format_string, concat_ws
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
trusted_tb_elenco = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_elenco").toDF()
trusted_tb_pessoa = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_pessoa").toDF()
trusted_tb_titulo = glueContext.create_dynamic_frame.from_catalog(database=base_dados, table_name="tb_titulo").toDF()

# Criar relação entre elenco e pessoa para garantir que dt_nascimento esteja disponível
df_elenco_pessoa = trusted_tb_elenco.alias("e") \
    .join(trusted_tb_pessoa.alias("p"), "id_pessoa", "left") \
    .select(
        col("e.id_titulo"),
        col("e.tp_titulo"),
        col("e.id_pessoa"),
        col("p.dt_nascimento")  # Preserva a data de nascimento do ator/atriz
    )

# Criar a Fato Pessoa com agregações baseadas no elenco e nos títulos
fato_pessoa = df_elenco_pessoa.alias("ep") \
    .join(trusted_tb_titulo.alias("t"), 
          (col("ep.id_titulo") == col("t.id_titulo")) & 
          (col("ep.tp_titulo") == col("t.tp_titulo")), 
          "left") \
    .groupBy("ep.id_pessoa", "ep.dt_nascimento") \
    .agg(
        count("ep.id_titulo").alias("qt_titulos_participacao"),  # Quantidade de títulos em que a pessoa participou
        sum(col("t.qt_votos")).alias("qt_votos_titulos"),  # Soma total de votos recebidos pelos títulos
        avg(col("t.vl_nota_media")).alias("vl_media_notas_titulos"),  # Média das notas dos títulos
        avg(year(col("t.dt_lancamento")) - year(col("ep.dt_nascimento"))).alias("vl_idade_atuacao_media")  # Média de idade de atuação
    )

# Criar a coluna id_tempo baseada na data de nascimento (dt_nascimento)
fato_pessoa = fato_pessoa.withColumn(
    "id_tempo", 
    concat_ws("", 
              format_string("%04d", year(col("dt_nascimento"))),
              format_string("%02d", month(col("dt_nascimento"))),
              format_string("%02d", dayofmonth(col("dt_nascimento")))
             ).cast("int")
)

# Exibir a estrutura e as primeiras linhas para verificação
fato_pessoa.show(10, False)
fato_pessoa.printSchema()

# Converter para DynamicFrame para escrita no S3
fato_pessoa_dyf = DynamicFrame.fromDF(fato_pessoa, glueContext, "fato_pessoa")

# Escrever a Fato Pessoa na camada Refined no formato Parquet
glueContext.write_dynamic_frame.from_options(
    frame=fato_pessoa_dyf, 
    connection_type="s3", 
    connection_options={"path": "s3://desafio-final.data-lake/Refined/fato_pessoa/"},
    format="parquet"
)

# Finalizar o Job no Glue
job.commit()
