import requests
import json
import os
import pytz
import boto3
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

# Função para encontrar a partição mais recente dinamicamente
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

# Função para baixar o país de origem dos filmes
def baixar_pais_origem(filmes_ids, max_por_arquivo=100):
    pais_origem = []
    
    for i, filme_id in enumerate(filmes_ids, start=1):
        url = f"{url_base}/movie/{filme_id}?api_key={api_key}&language={idioma}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()
            pais = dados.get("origin_country", [])
            
            pais_origem.append({
                "movie_id": filme_id,
                "origin_country": pais
            })

            print(f"Informações baixadas para o filme {filme_id} ({i}/{len(filmes_ids)})")
        else:
            print(f"Erro ao baixar informações do filme {filme_id}. Status: {response.status_code}")

    # Obtendo data atual para nomear arquivos corretamente
    agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/pais_origem_filmes/{ano}/{mes}/{dia}/"

    # Dividindo em arquivos de no máximo `max_por_arquivo`
    arquivos_criados = 0
    for i in range(0, len(pais_origem), max_por_arquivo):
        bloco = pais_origem[i:i + max_por_arquivo]
        arquivo_nome = f"pais_origem_filmes_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

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
        baixar_pais_origem(filmes_ids, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }
