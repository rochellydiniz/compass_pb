
# Importar bibliotecas
from dotenv import load_dotenv
import os
import boto3
import requests

# Carregar as variáveis da biblioteca .env
load_dotenv()

# Cadastrar as credenciais da AWS
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Criar um cliente S3 usando a sessão boto3
s3_client = session.client('s3')

# Listar os buckets S3 disponíveis na conta AWS
buckets = s3_client.list_buckets()

print("Buckets disponíveis:")
for bucket in buckets['Buckets']:
    print(f"  - {bucket['Name']}")

# Endereço do arquivo para download
url = "https://www.dados.cefetmg.br/wp-content/uploads/sites/248/2023/02/PDA_2022-2024_1.2_Alunos_Anonimo.csv"

# Nome do arquivo e do bucket S3
nome_arquivo_s3 = "PDA_2022-2024_1.2_Alunos_Anonimo.csv"
nome_bucket = "desafio-dados.abertos"
s3_key = nome_arquivo_s3


try:
    # Baixar o arquivo
    print(f"Baixando o arquivo de {url}...")
    response = requests.get(url)
    response.raise_for_status()

    # Salvar o arquivo localmente
    with open(nome_arquivo_s3, "wb") as file:
        file.write(response.content)
    print(f"Arquivo salvo localmente como {nome_arquivo_s3}.")

    # Subir para o bucket S3
    print(f"Subindo para o bucket S3 '{nome_bucket}'...")
    s3_client.upload_file(nome_arquivo_s3, nome_bucket, s3_key)
    print(f"Envio concluído. Arquivo salvo como '{s3_key}' no bucket '{nome_bucket}'.")

except requests.exceptions.RequestException as e:
    print(f"Erro ao baixar o arquivo: {e}")

except boto3.exceptions.S3UploadFailedError as e:
    print(f"Erro ao subir arquivo para o S3: {e}")

