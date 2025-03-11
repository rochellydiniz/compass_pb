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
nome_bucket = "desafio-final.data-lake"

diretorios_arquivos_base = [
    "RAW Zone/TMDB/JSON/creditos_filmes/",
    "RAW Zone/TMDB/JSON/creditos_series/"
]

# Configurar cliente S3
s3_client = boto3.client("s3")

# Função para encontrar a partição mais recente para um diretório base
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

# Função para carregar IDs de pessoas a partir das partições mais recentes
def carregar_pessoas_s3():
    """Lê os arquivos de créditos do S3 e retorna as IDs das pessoas."""
    pessoas_ids = set()

    for diretorio_base in diretorios_arquivos_base:
        ultima_particao = obter_ultima_particao(nome_bucket, diretorio_base)
        if not ultima_particao:
            print(f"Nenhuma partição encontrada para {diretorio_base}. Pulando...")
            continue

        print(f"Usando partição mais recente: {ultima_particao}")
        arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=ultima_particao)

        if "Contents" not in arquivos:
            print(f"Nenhum arquivo encontrado em {ultima_particao}.")
            continue

        for obj in arquivos.get("Contents", []):
            key = obj["Key"]
            response = s3_client.get_object(Bucket=nome_bucket, Key=key)
            creditos = json.loads(response["Body"].read().decode("utf-8"))

            for item in creditos:
                for ator in item.get("elenco_principal", []):
                    if ator.get("id"):
                        pessoas_ids.add(ator["id"])
                for diretor in item.get("direcao", []):
                    if diretor.get("id"):
                        pessoas_ids.add(diretor["id"])

    print(f"{len(pessoas_ids)} pessoas carregadas do S3.")
    return list(pessoas_ids)

# Função para baixar informações de uma única pessoa
def baixar_info_pessoa(pessoa_id):
    """Faz a requisição para obter informações de uma pessoa."""
    url = f"{url_base}/person/{pessoa_id}?api_key={api_key}&language=pt-BR"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao baixar informações da pessoa {pessoa_id}. Status: {response.status_code}")
        return None

# Função para baixar informações das pessoas em paralelo e por lotes de 100
def baixar_info_pessoas_parallel(pessoas_ids, max_workers=50, max_por_arquivo=100):
    """Baixa os dados das pessoas usando paralelismo e salvamento incremental."""
    total_pessoas = len(pessoas_ids)
    numero_arquivo = 1  # Começa a numeração dos arquivos em 1

    # Divide as pessoas em lotes de 100
    for i in range(0, total_pessoas, max_por_arquivo):
        lote = pessoas_ids[i:i + max_por_arquivo]
        print(f"Processando lote {i // max_por_arquivo + 1} de {total_pessoas // max_por_arquivo + 1}...")

        pessoas_info = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(baixar_info_pessoa, pid): pid for pid in lote}

            for future in concurrent.futures.as_completed(futuros):
                result = future.result()
                if result:
                    pessoas_info.append(result)

        print(f"Lote {i // max_por_arquivo + 1} processado! Total acumulado: {len(pessoas_info)} pessoas.")

        # Salvamento incremental no S3
        salvar_pessoas_no_s3(pessoas_info, numero_arquivo)
        numero_arquivo += 1  # Incrementa a numeração dos arquivos

# Função para salvar as informações no S3 com numeração correta
def salvar_pessoas_no_s3(pessoas_info, numero_arquivo):
    """Salva as informações das pessoas no S3 mantendo a numeração dos arquivos."""
    if not pessoas_info:
        return
    
    agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/pessoas/{ano}/{mes}/{dia}/"

    arquivo_nome = f"pessoas_{numero_arquivo}.json"
    caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

    s3_client.put_object(
        Bucket=nome_bucket,
        Key=caminho_s3,
        Body=json.dumps(pessoas_info, ensure_ascii=False, indent=4),
        ContentType="application/json"
    )

    print(f"Arquivo salvo no S3: {caminho_s3} com {len(pessoas_info)} pessoas.")

# Função principal do AWS Lambda
def lambda_handler(event, context):
    """Função principal do AWS Lambda."""
    pessoas_ids = carregar_pessoas_s3()

    if not pessoas_ids:
        print("Nenhuma pessoa encontrada nos créditos salvos no S3!")
        return {"statusCode": 200, "body": json.dumps("Nenhuma pessoa encontrada nos créditos salvos no S3!")}

    baixar_info_pessoas_parallel(pessoas_ids, max_workers=50, max_por_arquivo=100)

    return {"statusCode": 200, "body": json.dumps(f"{len(pessoas_ids)} pessoas processadas.")}
