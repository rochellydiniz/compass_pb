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

# Função para baixar créditos de um único filme
def baixar_creditos_individual(filme_id):
    url = f"{url_base}/movie/{filme_id}/credits?api_key={api_key}&language={idioma}"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        elenco_principal = [
            {"id": ator.get("id"), "nome": ator.get("name", "Desconhecido"), "personagem": ator.get("character", "N/A")}
            for ator in dados.get("cast", [])[:6]
        ] if dados.get("cast") else []

        direcao = [
            {"id": membro.get("id"), "nome": membro.get("name", "Desconhecido")}
            for membro in dados.get("crew", []) if membro.get("job") == "Director"
        ] if dados.get("crew") else []

        return {"movie_id": filme_id, "elenco_principal": elenco_principal, "direcao": direcao}
    else:
        print(f"Erro ao baixar créditos do filme {filme_id}. Status: {response.status_code}")
        return None

# Função para baixar créditos dos filmes em lotes e em paralelo
def baixar_creditos(filmes_ids, max_workers=20, max_por_arquivo=100):
    creditos = []
    total_filmes = len(filmes_ids)

    # Dividir filmes em lotes para evitar sobrecarga
    for i in range(0, total_filmes, max_por_arquivo):
        lote = filmes_ids[i:i + max_por_arquivo]
        print(f"Processando lote {i // max_por_arquivo + 1} de {total_filmes // max_por_arquivo + 1}...")

        # Paralelismo controlado para evitar uso excessivo de memória
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(baixar_creditos_individual, filme_id): filme_id for filme_id in lote}

            for future in as_completed(futuros):
                result = future.result()
                if result:
                    creditos.append(result)

        print(f"Lote {i // max_por_arquivo + 1} processado! Total acumulado: {len(creditos)} filmes.")

        # Salvamento incremental a cada lote processado
        salvar_creditos_no_s3(creditos)
        creditos = []  # Limpa a lista para liberar memória

# Função para salvar os créditos no S3
def salvar_creditos_no_s3(creditos):
    """Divide as informações e salva no S3."""
    if not creditos:
        return
    
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/creditos_filmes/{ano}/{mes}/{dia}/"

    arquivo_nome = f"creditos_filmes_{datetime.now().strftime('%H%M%S')}.json"
    caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

    s3_client.put_object(
        Bucket=nome_bucket,
        Key=caminho_s3,
        Body=json.dumps(creditos, ensure_ascii=False, indent=4),
        ContentType="application/json"
    )

    print(f"Arquivo salvo no S3: {caminho_s3} com {len(creditos)} filmes.")

# Função principal do AWS Lambda
def lambda_handler(event, context):
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_creditos(filmes_ids, max_workers=20, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }
import requests
import json
import os
import pytz
from datetime import datetime
from tmdbv3api import TMDb
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração global
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

# Função para baixar créditos de um único filme
def baixar_creditos_individual(filme_id):
    url = f"{url_base}/movie/{filme_id}/credits?api_key={api_key}&language={idioma}"
    response = requests.get(url)

    if response.status_code == 200:
        dados = response.json()
        elenco_principal = [
            {"id": ator.get("id"), "nome": ator.get("name", "Desconhecido"), "personagem": ator.get("character", "N/A")}
            for ator in dados.get("cast", [])[:6]
        ] if dados.get("cast") else []

        direcao = [
            {"id": membro.get("id"), "nome": membro.get("name", "Desconhecido")}
            for membro in dados.get("crew", []) if membro.get("job") == "Director"
        ] if dados.get("crew") else []

        return {"movie_id": filme_id, "elenco_principal": elenco_principal, "direcao": direcao}
    else:
        print(f"Erro ao baixar créditos do filme {filme_id}. Status: {response.status_code}")
        return None

# Função para baixar créditos dos filmes em lotes e em paralelo
def baixar_creditos(filmes_ids, max_workers=20, max_por_arquivo=100):
    creditos = []
    total_filmes = len(filmes_ids)
    numero_arquivo = 1  # Começa a numeração dos arquivos em 1

    # Dividir filmes em lotes para evitar sobrecarga
    for i in range(0, total_filmes, max_por_arquivo):
        lote = filmes_ids[i:i + max_por_arquivo]
        print(f"Processando lote {i // max_por_arquivo + 1} de {total_filmes // max_por_arquivo + 1}...")

        # Paralelismo controlado para evitar uso excessivo de memória
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(baixar_creditos_individual, filme_id): filme_id for filme_id in lote}

            for future in as_completed(futuros):
                result = future.result()
                if result:
                    creditos.append(result)

        print(f"Lote {i // max_por_arquivo + 1} processado! Total acumulado: {len(creditos)} filmes.")

        # Salvamento incremental a cada lote processado
        salvar_creditos_no_s3(creditos, numero_arquivo)
        creditos = []  # Limpa a lista para liberar memória
        numero_arquivo += 1  # Incrementa a numeração dos arquivos

# Função para salvar os créditos no S3 com numeração correta
def salvar_creditos_no_s3(creditos, numero_arquivo):
    """Divide as informações e salva no S3 com numeração correta."""
    if not creditos:
        return
    
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/creditos_filmes/{ano}/{mes}/{dia}/"

    arquivo_nome = f"creditos_filmes_{numero_arquivo}.json"
    caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

    s3_client.put_object(
        Bucket=nome_bucket,
        Key=caminho_s3,
        Body=json.dumps(creditos, ensure_ascii=False, indent=4),
        ContentType="application/json"
    )

    print(f"Arquivo salvo no S3: {caminho_s3} com {len(creditos)} filmes.")

# Função principal do AWS Lambda
def lambda_handler(event, context):
    filmes_ids = carregar_filmes_s3()
    if filmes_ids:
        baixar_creditos(filmes_ids, max_workers=20, max_por_arquivo=100)
    else:
        print("Nenhum filme encontrado no S3!")

    return {
        "statusCode": 200,
        "body": json.dumps(f"{len(filmes_ids)} filmes processados.")
    }
