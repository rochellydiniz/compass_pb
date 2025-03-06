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

def carregar_series_s3():
    # Lê os arquivos de series do S3 e retorna os IDs dos series.
    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix="RAW Zone/TMDB/JSON/series/2025/02/08/")
    series_ids = []

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        series = json.loads(response["Body"].read().decode("utf-8"))
        
        series_ids.extend([serie["id"] for serie in series])

    print(f"{len(series_ids)} series carregados do S3.")
    return series_ids


def baixar_ids_externos(series_ids, max_por_arquivo=100):
    
    ids_externos = []
    
    # IDs externos das series
    for i, serie_id in enumerate(series_ids, start=1):
        url = f"{url_base}/tv/{serie_id}/external_ids?api_key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()
            
            ids_externos.append({
                "movie_id": serie_id,
                "imdb_id": dados.get("imdb_id"),
            })

            print(f"Informações baixadas para o serie {serie_id} ({i}/{len(series_ids)})")
        else:
            print(f"Erro ao baixar informações do serie {serie_id}. Status: {response.status_code}")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/ids_externos_series/{ano}/{mes}/{dia}/"

    # Dividir em blocos de max_por_arquivo
    arquivos_criados = 0
    for i in range(0, len(ids_externos), max_por_arquivo):
        bloco = ids_externos[i:i + max_por_arquivo]
        arquivo_nome = f"id_externo_series_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

        # Salvar no S3
        s3_client.put_object(
            Bucket=nome_bucket,
            Key=caminho_s3,
            Body=json.dumps(bloco, ensure_ascii=False, indent=4),
            ContentType="application/json"
        )

        arquivos_criados += 1
        print(f"Arquivo salvo no S3: {caminho_s3} com {len(bloco)} series.")

    print(f"{arquivos_criados} arquivos JSON criados no S3!")

def lambda_handler(event, context):
    """Função principal do AWS Lambda"""
    series_ids = carregar_series_s3()
    if series_ids:
        baixar_ids_externos(series_ids, max_por_arquivo=100)
    else:
        print("Nenhum serie encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(series_ids)} series processados.")
    }