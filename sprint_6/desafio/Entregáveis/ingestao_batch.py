# Implementar código Python:
# -> Ler os 2 arquivos (filmes e séries) no formato ``CSV`` inteiros,
#    ou seja, sem filtrar os dados.
# -> Utilizar a lib ``boto3`` para carregar os dados para a AWS.
# -> Acessar a AWS e gravar no S3, no bucket definido com RAW Zone.
#    -> No momento da gravação dos dados, considerar o padrão: 
#       <nome do bucket>\<camada de armazenamento>\<origem do dado>\<formato do dado>\<especificação do dado>\<data de processamento separada por>\<ano>\<mes>\<dia>\<arquivo>.

# Importar bibliotecas necessárias
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import pytz
import boto3
import os

# Carregar variáveis da biblioteca .env
load_dotenv()


# Cadastrar credenciais da AWS
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Configurar o fuso horário
fuso_horario = pytz.timezone('America/Sao_Paulo')


# Criar cliente S3 usando a sessão boto3
s3_client = session.client('s3')


# Configurar o S3
nome_bucket = 'desafio-final.data-lake'


# Função para Ler os Arquivos CSV
def ler_arquivo(arquivo_caminho):
    return pd.read_arquivo(arquivo_caminho)


# Função para Enviar os Arquivos para o S3
def subir_para_s3(arquivo_caminho, tipo_dado):
    arquivo_nome = os.path.basename(arquivo_caminho)
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')

    # Criar o caminho do arquivo no S3
    caminho_s3 = f'RAW Zone/Local/CSV/{tipo_dado}/{ano}/{mes}/{dia}/{arquivo_nome}'
    s3_client.upload_file(arquivo_caminho, nome_bucket, caminho_s3)
    print(f'Arquivo {arquivo_nome} enviado para o S3 com sucesso!')

# Executar a função subir_para_s3
subir_para_s3('datasets/movies.csv', 'Filmes')
subir_para_s3('datasets/series.csv', 'Series')