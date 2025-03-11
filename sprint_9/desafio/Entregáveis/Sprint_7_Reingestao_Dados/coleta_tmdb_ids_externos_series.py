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
idioma = "pt-BR"
nome_bucket = 'desafio-final.data-lake'

# Configurar cliente S3
s3_client = boto3.client("s3")

# Função para encontrar a partição mais recente baseada nos arquivos disponíveis
def obter_ultima_particao(bucket, prefixo_base):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_base)

    if "Contents" not in response:
        print("Nenhuma partição encontrada!")
        return None

    datas_encontradas = []

    # Extrair a data do caminho dos arquivos
    for obj in response["Contents"]:
        caminho = obj["Key"].split("/")
        try:
            ano, mes, dia = int(caminho[-4]), int(caminho[-3]), int(caminho[-2])
            data = datetime(ano, mes, dia)
            datas_encontradas.append((data, f"{ano}/{mes:02d}/{dia:02d}"))
        except (ValueError, IndexError):
            continue  # Ignora arquivos que não seguem o formato esperado

    if not datas_encontradas:
        print("Nenhuma partição válida encontrada!")
        return None

    # Encontrar a data mais recente
    ultima_data = max(datas_encontradas, key=lambda x: x[0])[1]
    
    return f"{prefixo_base}{ultima_data}/"

# Função para carregar séries do S3
def carregar_series_s3():
    prefixo_base = "RAW Zone/TMDB/JSON/series/"
    ultima_particao = obter_ultima_particao(nome_bucket, prefixo_base)

    if not ultima_particao:
        print("Nenhuma partição encontrada para séries.")
        return []

    print(f"Usando partição mais recente: {ultima_particao}")

    # Listar arquivos na última partição
    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)
    series_ids = []

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        series = json.loads(response["Body"].read().decode("utf-8"))
        
        series_ids.extend([serie["id"] for serie in series])

    print(f"{len(series_ids)} séries carregadas do S3.")
    return series_ids

# Função para baixar IDs externos das séries
def baixar_ids_externos(series_ids, max_por_arquivo=100):
    ids_externos = []

    # IDs externos das séries
    for i, serie_id in enumerate(series_ids, start=1):
        url = f"{url_base}/tv/{serie_id}/external_ids?api_key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()
            
            ids_externos.append({
                "serie_id": serie_id,
                "imdb_id": dados.get("imdb_id"),
            })

            print(f"Informações baixadas para a série {serie_id} ({i}/{len(series_ids)})")
        else:
            print(f"Erro ao baixar informações da série {serie_id}. Status: {response.status_code}")

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
        print(f"Arquivo salvo no S3: {caminho_s3} com {len(bloco)} séries.")

    print(f"🎉 {arquivos_criados} arquivos JSON criados no S3!")

# Função principal do AWS Lambda
def lambda_handler(event, context):
    series_ids = carregar_series_s3()
    if series_ids:
        baixar_ids_externos(series_ids, max_por_arquivo=100)
    else:
        print("Nenhuma série encontrada no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(series_ids)} séries processadas.")
    }
