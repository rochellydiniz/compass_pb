import requests
import json
import os
import pytz
import boto3
from datetime import datetime
from tmdbv3api import TMDb

# Configuração da API TMDB
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY')

# Configurações globais
api_key = os.getenv('TMDB_API_KEY')
url_base = "https://api.themoviedb.org/3"
idioma = "pt-BR"
nome_bucket = 'desafio-final.data-lake'

# Configurar cliente S3
s3_client = boto3.client("s3")

# Função para encontrar a última partição disponível no S3
def obter_ultima_particao(bucket, prefixo_base):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_base)

    if "Contents" not in response:
        print(f"Nenhuma partição encontrada para {prefixo_base}!")
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
        print(f"Nenhuma partição válida encontrada para {prefixo_base}!")
        return None

    ultima_data = max(datas_encontradas, key=lambda x: x[0])[1]
    return f"{prefixo_base}{ultima_data}/"

# Função para carregar os IDs das séries do S3
def carregar_series_s3():
    prefixo_base = "RAW Zone/TMDB/JSON/series/"
    ultima_particao = obter_ultima_particao(nome_bucket, prefixo_base)

    if not ultima_particao:
        print("Nenhuma partição encontrada para séries.")
        return []

    print(f"Usando partição mais recente: {ultima_particao}")

    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)
    series_ids = []

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        series = json.loads(response["Body"].read().decode("utf-8"))

        series_ids.extend([serie["id"] for serie in series])

    print(f"{len(series_ids)} séries carregadas do S3.")
    return series_ids

# Função para baixar a classificação indicativa das séries
def baixar_class_indicativa(series_ids, max_por_arquivo=100):
    class_indicativa = []

    for i, serie_id in enumerate(series_ids, start=1):
        url = f"{url_base}/tv/{serie_id}/content_ratings?api_key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()
            class_indicativa.append({
                "serie_id": serie_id,
                "classificacao": dados.get("results", [])
            })
            print(f"Classificação indicativa baixada para a série {serie_id} ({i}/{len(series_ids)})")
        else:
            print(f"Erro ao baixar infos da série {serie_id}. Status: {response.status_code}")

    # Criar caminho da pasta de saída com a data atual
    agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/class_indicativa_series/{ano}/{mes}/{dia}/"

    # Dividir e salvar os arquivos no S3
    arquivos_criados = 0
    for i in range(0, len(class_indicativa), max_por_arquivo):
        bloco = class_indicativa[i:i + max_por_arquivo]
        arquivo_nome = f"class_ind_series_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

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
        baixar_class_indicativa(series_ids, max_por_arquivo=100)
    else:
        print("Nenhuma série encontrada no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(series_ids)} séries processadas.")
    }
