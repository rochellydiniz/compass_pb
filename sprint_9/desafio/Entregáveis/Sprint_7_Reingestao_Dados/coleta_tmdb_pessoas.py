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

diretorios_arquivos = [
    "RAW Zone/TMDB/JSON/creditos_filmes/2025/02/12/",
    "RAW Zone/TMDB/JSON/creditos_series/2025/02/12/"
]

# Configurar cliente S3
s3_client = boto3.client("s3")

def carregar_pessoas_s3():
    """Lê os arquivos de créditos do S3 e retorna as IDs das pessoas."""
    pessoas_ids = set()
    for diretorio in diretorios_arquivos:
        arquivos = s3_client.list_objects_v2(Bucket=nome_bucket, Prefix=diretorio)
        if "Contents" not in arquivos:
            print(f"Nenhum arquivo encontrado em {diretorio}.")
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

def baixar_info_pessoa(pessoa_id):
    """Faz a requisição para obter informações de uma pessoa."""
    url = f"{url_base}/person/{pessoa_id}?api_key={api_key}&language=pt-BR"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao baixar informações da pessoa {pessoa_id}. Status: {response.status_code}")
        return None

def baixar_info_pessoas_parallel(pessoas_ids, max_workers=10, max_por_arquivo=100):
    """Baixa os dados das pessoas usando paralelismo."""
    pessoas_info = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {executor.submit(baixar_info_pessoa, pid): pid for pid in pessoas_ids}
        for future in concurrent.futures.as_completed(future_to_id):
            result = future.result()
            if result:
                pessoas_info.append(result)
    salvar_pessoas_no_s3(pessoas_info, max_por_arquivo)

def salvar_pessoas_no_s3(pessoas_info, max_por_arquivo):
    """Divide as informações e salva no S3."""
    agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/pessoas/{ano}/{mes}/{dia}/"
    
    for i in range(0, len(pessoas_info), max_por_arquivo):
        bloco = pessoas_info[i:i + max_por_arquivo]
        arquivo_nome = f"pessoas_{(i // max_por_arquivo) + 1}.json"
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"
        s3_client.put_object(
            Bucket=nome_bucket,
            Key=caminho_s3,
            Body=json.dumps(bloco, ensure_ascii=False, indent=4),
            ContentType="application/json"
        )
        print(f"Arquivo salvo no S3: {caminho_s3} com {len(bloco)} pessoas.")

def lambda_handler(event, context):
    """Função principal do AWS Lambda."""
    pessoas_ids = carregar_pessoas_s3()
    if not pessoas_ids:
        print("Nenhuma pessoa encontrada nos créditos salvos no S3!")
        return {"statusCode": 200, "body": json.dumps("Nenhuma pessoa encontrada nos créditos salvos no S3!")}
    
    baixar_info_pessoas_parallel(pessoas_ids, max_workers=10, max_por_arquivo=100)
    return {"statusCode": 200, "body": json.dumps(f"{len(pessoas_ids)} pessoas processadas.")}
