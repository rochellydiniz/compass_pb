import requests
import json
import os
import pytz
import boto3
import concurrent.futures
from datetime import datetime
from tmdbv3api import TMDb

# Configuração da API TMDB
tmdb = TMDb()
tmdb.api_key = os.getenv("TMDB_API_KEY")

# Configurações globais
api_key = os.getenv("TMDB_API_KEY")
url_base = "https://api.themoviedb.org/3"
idioma = "pt-BR"
nome_bucket = "desafio-final.data-lake"

# Configurar cliente S3
s3_client = boto3.client("s3")

# Função para buscar a partição mais recente
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

# Função para carregar IDs dos filmes do S3
def carregar_filmes_s3():
    prefixo_base = "RAW Zone/TMDB/JSON/filmes/"
    ultima_particao = obter_ultima_particao(nome_bucket, prefixo_base)

    if not ultima_particao:
        print("Nenhuma partição encontrada para filmes.")
        return []

    print(f"Usando partição mais recente: {ultima_particao}")

    arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)
    filmes_ids = []

    for obj in arquivos.get("Contents", []):
        key = obj["Key"]
        response = s3_client.get_object(Bucket=nome_bucket, Key=key)
        filmes = json.loads(response["Body"].read().decode("utf-8"))
        filmes_ids.extend([filme["id"] for filme in filmes])

    print(f"{len(filmes_ids)} filmes carregados do S3.")
    return filmes_ids

# Função para baixar os IDs externos de um filme
def baixar_id_externo_filme(filme_id):
    url = f"{url_base}/movie/{filme_id}/external_ids?api_key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        return {
            "movie_id": filme_id,
            "imdb_id": dados.get("imdb_id"),
        }
    else:
        print(f"Erro ao baixar informações do filme {filme_id}. Status: {response.status_code}")
        return None

# Função para processar os filmes em paralelo com 512MB de RAM
def baixar_ids_externos_parallel(filmes_ids, max_workers=10, max_por_arquivo=100):
    """Baixa os IDs externos dos filmes usando paralelismo otimizado para 512MB."""
    ids_externos = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {executor.submit(baixar_id_externo_filme, filme_id): filme_id for filme_id in filmes_ids}
        
        for future in concurrent.futures.as_completed(futuros):
            result = future.result()
            if result:
                ids_externos.append(result)

    print(f"IDs externos baixados para {len(ids_externos)} filmes.")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/ids_externos_filmes/{ano}/{mes}/{dia}/"

    # Dividindo em arquivos de no máximo `max_por_arquivo`
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

# Função principal do AWS Lambda
def lambda_handler(event, context):
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_ids_externos_parallel(filmes_ids, max_workers=10, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }
