import requests
import json
import os
import pytz
from datetime import datetime
from tmdbv3api import TMDb
import boto3

# Configurações globais
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY')

fuso_horario = pytz.timezone('America/Sao_Paulo')
api_key = os.getenv('TMDB_API_KEY')
url_base = "https://api.themoviedb.org/3"
idioma = "pt-BR"
nome_bucket = 'desafio-final.data-lake'

# Configurar cliente S3
s3_client = boto3.client("s3")

# Função para encontrar a partição mais recente
def obter_ultima_particao(bucket, prefixo_base):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_base)

    if "Contents" not in response:
        print("Nenhuma partição encontrada!")
        return None

    datas_encontradas = []
    for obj in response["Contents"]:
        caminho = obj["Key"].split("/")
        try:
            ano, mes, dia = int(caminho[-4]), int(caminho[-3]), int(caminho[-2])
            data = datetime(ano, mes, dia)
            datas_encontradas.append((data, f"{ano}/{mes:02d}/{dia:02d}"))
        except (ValueError, IndexError):
            continue  
            
    if not datas_encontradas:
        print("Nenhuma partição válida encontrada!")
        return None

    ultima_data = max(datas_encontradas, key=lambda x: x[0])[1]
    return f"{prefixo_base}{ultima_data}/"

# Função para carregar séries do S3
def carregar_series_s3():
    prefixo_base = "RAW Zone/TMDB/JSON/series/"
    ultima_particao = obter_ultima_particao(nome_bucket, prefixo_base)

    if not ultima_particao:
        print("Nenhuma partição encontrada para séries.")
        return set()

    print(f"Usando partição mais recente: {ultima_particao}")

    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)
    series_ids = set()

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        series = json.loads(response["Body"].read().decode("utf-8"))
        series_ids.update([serie["id"] for serie in series])

    print(f"{len(series_ids)} séries carregadas do S3.")
    return series_ids

# Função para baixar keywords de uma única série
def baixar_keywords_individual(serie_id):
    url = f"{url_base}/tv/{serie_id}/keywords?api_key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        return {
            "id": serie_id,
            "results": dados.get("results", [])
        }
    else:
        print(f"Erro ao baixar palavras-chave da série {serie_id}. Status: {response.status_code}")
        return None

# Função para baixar keywords das séries
def baixar_keywords_serie(series_ids, max_por_arquivo=100):
    results = []

    # Baixar keywords em paralelo (ThreadPoolExecutor pode ser adicionado se necessário)
    for i, serie_id in enumerate(series_ids, start=1):
        result = baixar_keywords_individual(serie_id)
        if result:
            results.append(result)
        print(f"Palavras-chave baixadas para a série {serie_id} ({i}/{len(series_ids)})")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/keywords_series/{ano}/{mes}/{dia}/"

    # Dividir em blocos de max_por_arquivo
    arquivos_criados = 0
    for i in range(0, len(results), max_por_arquivo):
        bloco = results[i:i + max_por_arquivo]
        arquivo_nome = f"keywords_series_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

        # Salvar no S3
        s3_client.put_object(
            Bucket=nome_bucket,
            Key=caminho_s3,
            Body=json.dumps(bloco, ensure_ascii=False, indent=4),
            ContentType="application/json"
        )

        arquivos_criados += 1
        print(f"Arquivo salvo no S3: {caminho_s3} com {len(bloco)} séries.")

    print(f"{arquivos_criados} arquivos JSON criados no S3!")

# Função principal do AWS Lambda
def lambda_handler(event, context):
    series_ids = carregar_series_s3()
    if series_ids:
        baixar_keywords_serie(series_ids, max_por_arquivo=100)
    else:
        print("Nenhuma série encontrada no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(series_ids)} séries processadas.")
    }

