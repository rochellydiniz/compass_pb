import requests
import requests
import json
import os
import pytz
from datetime import datetime
from tmdbv3api import TMDb
import boto3


# Cadastrar credencial TMDB
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY')

# Configurar o fuso horário
fuso_horario = pytz.timezone('America/Sao_Paulo')


# Configurações globais
api_key = os.getenv('TMDB_API_KEY')
url_base = "https://api.themoviedb.org/3"
generos = "9648"  # Mistério (9648)
idioma = "pt-BR"
nome_bucket = 'desafio-final.data-lake'

# Configurar cliente S3
s3_client = boto3.client("s3")

def carregar_filmes_s3():
    # Lê os arquivos de filmes do S3 e retorna os IDs dos filmes.
    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix="RAW Zone/TMDB/JSON/filmes/2025/02/08/")
    filmes_ids = []

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        filmes = json.loads(response["Body"].read().decode("utf-8"))
        
        filmes_ids.extend([filme["id"] for filme in filmes])

    print(f"{len(filmes_ids)} filmes carregados do S3.")
    return filmes_ids


def baixar_ids_externos(filmes_ids, max_por_arquivo=100):
    
    ids_externos = []
    
    # IDs externos dos filmes
    for i, filme_id in enumerate(filmes_ids, start=1):
        url = f"{url_base}/movie/{filme_id}/external_ids?api_key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()

          
            ids_externos.append({
                "movie_id": filme_id,
                "imdb_id": dados.get("imdb_id"),
            })

            print(f"Informações baixadas para o filme {filme_id} ({i}/{len(filmes_ids)})")
        else:
            print(f"Erro ao baixar informações do filme {filme_id}. Status: {response.status_code}")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/ids_externos_filmes/{ano}/{mes}/{dia}/"

    # Dividir em blocos de max_por_arquivo
    arquivos_criados = 0
    for i in range(0, len(ids_externos), max_por_arquivo):
        bloco = ids_externos[i:i + max_por_arquivo]
        arquivo_nome = f"id_externo_filmes_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

        # Salvar no S3
        s3_client.put_object(
            Bucket=nome_bucket,
            Key=caminho_s3,
            Body=json.dumps(bloco, ensure_ascii=False, indent=4),
            ContentType="application/json"
        )

        arquivos_criados += 1
        print(f"Arquivo salvo no S3: {caminho_s3} com {len(bloco)} filmes.")

    print(f"{arquivos_criados} arquivos JSON criados no S3!")

def lambda_handler(event, context):
    """Função principal do AWS Lambda"""
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_ids_externos(filmes_ids, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }