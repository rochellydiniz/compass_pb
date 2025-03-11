import requests
import json
import os
import pytz
from datetime import datetime
from tmdbv3api import TMDb
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Função para carregar filmes do S3
def carregar_filmes_s3():
    prefixo_base = "RAW Zone/TMDB/JSON/filmes/"
    ultima_particao = obter_ultima_particao(nome_bucket, prefixo_base)

    if not ultima_particao:
        print("Nenhuma partição encontrada para filmes.")
        return set()

    print(f"Usando partição mais recente: {ultima_particao}")

    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)
    filmes_ids = set()

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        filmes = json.loads(response["Body"].read().decode("utf-8"))
        filmes_ids.update([filme["id"] for filme in filmes])

    print(f"{len(filmes_ids)} filmes carregados do S3.")
    return filmes_ids

# Função para baixar keywords de um único filme
def baixar_keywords_individual(filme_id):
    url = f"{url_base}/movie/{filme_id}/keywords?api_key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        return {
            "movie_id": filme_id,
            "keywords": dados.get("keywords", [])
        }
    else:
        print(f"Erro ao baixar infos do filme {filme_id}. Status: {response.status_code}")
        return None

# Função para baixar keywords de filmes em paralelo
def baixar_keywords_filme(filmes_ids, max_por_arquivo=100):
    keywords = []
    
    # Usar ThreadPoolExecutor para processar em paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        futuros = {executor.submit(baixar_keywords_individual, filme_id): filme_id for filme_id in filmes_ids}
        
        for future in as_completed(futuros):
            result = future.result()
            if result:
                keywords.append(result)

    print(f"Keywords baixadas para {len(keywords)} filmes.")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/keywords_filmes/{ano}/{mes}/{dia}/"

    # Dividir em blocos de max_por_arquivo
    arquivos_criados = 0
    for i in range(0, len(keywords), max_por_arquivo):
        bloco = keywords[i:i + max_por_arquivo]
        arquivo_nome = f"keywords_filmes_{(i // max_por_arquivo) + 1}.json"
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

# Função principal do AWS Lambda
def lambda_handler(event, context):
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_keywords_filme(filmes_ids, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }
