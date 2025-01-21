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


# 1. Ler o arquivo nomes.csv no S3
input_path = "s3://s7-lab-glue/input/nomes.csv"
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True}
)

# 2. Imprimir o schema do dataframe
print("Schema do DataFrame:")
df.printSchema()

# 3. Alterar a coluna "nome" para maiúsculas
from pyspark.sql.functions import col, upper

df_transformed = df.toDF().withColumn("nome", upper(col("nome")))

# 4. Imprimir a contagem de linhas no DataFrame
print("Número total de linhas no DataFrame:", df_transformed.count())

# 5. Contar os nomes agrupados por ano e sexo, ordenados pelo ano mais recente
from pyspark.sql.functions import count

grouped_df = df_transformed.groupBy("ano", "sexo").agg(count("nome").alias("quantidade")).orderBy(col("ano").desc())
grouped_df.show()

# 6. Nome feminino com mais registros e ano
feminino_df = df_transformed.filter(col("sexo") == "F")
feminino_top = feminino_df.groupBy("nome", "ano").count().orderBy(col("count").desc()).first()
print(f"Nome feminino mais comum: {feminino_top['nome']}, Ano: {feminino_top['ano']}")

# 7. Nome masculino com mais registros e ano
masculino_df = df_transformed.filter(col("sexo") == "M")
masculino_top = masculino_df.groupBy("nome", "ano").count().orderBy(col("count").desc()).first()
print(f"Nome masculino mais comum: {masculino_top['nome']}, Ano: {masculino_top['ano']}")

# 8. Total de registros masculinos e femininos por ano (apenas os 10 primeiros)
total_por_ano = df_transformed.groupBy("ano", "sexo").count().orderBy(col("ano")).limit(10)
total_por_ano.show()

# 9. Escrever os dados no S3 com particionamento por sexo e ano
output_path = "s3://s7-lab-glue/frequencia_registro_nomes_eua/"
df_transformed.write.partitionBy("sexo", "ano").mode("overwrite").json(output_path)



job.commit()