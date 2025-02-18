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

    print(f"📂 {len(filmes_ids)} filmes carregados do S3.")
    return filmes_ids


def baixar_creditos(filmes_ids, max_por_arquivo=100):
    # Baixa os créditos dos filmes
    creditos = []
    
    # Informações do elenco principal e direção
    for i, filme_id in enumerate(filmes_ids, start=1):
        url = f"{url_base}/movie/{filme_id}/credits?api_key={api_key}&language={idioma}"
        response = requests.get(url)

        if response.status_code == 200:
            dados = response.json()
            
            elenco_principal = [
                {"id": ator.get("id", None), "nome": ator.get("name", "Desconhecido"), "personagem": ator.get("character", "N/A")} 
                for ator in dados.get("cast", [])[:6]
            ] if dados.get("cast") else []

            direcao = [
                {"id": membro.get("id", None), "nome": membro.get("name", "Desconhecido")}
                for membro in dados.get("crew", [])
                if membro.get("job") == "Director"

            ] if dados.get("crew") else []


            creditos.append({
                "movie_id": filme_id,
                "elenco_principal": elenco_principal,
                "direcao": direcao
            })

            print(f"Créditos baixados para o filme {filme_id} ({i}/{len(filmes_ids)})")
        else:
            print(f"Erro ao baixar créditos do filme {filme_id}. Status: {response.status_code}")

    # Obter data atual para nomeação dos arquivos
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/creditos_filmes/{ano}/{mes}/{dia}/"

    # Dividir em blocos de max_por_arquivo
    arquivos_criados = 0
    for i in range(0, len(creditos), max_por_arquivo):
        bloco = creditos[i:i + max_por_arquivo]
        arquivo_nome = f"creditos_filmes_{(i // max_por_arquivo) + 1}.json"
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

    print(f"{arquivos_criados} arquivos JSON de créditos criados no S3!")

def lambda_handler(event, context):
    """Função principal do AWS Lambda"""
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_creditos(filmes_ids, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }