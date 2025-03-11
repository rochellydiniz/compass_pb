from tmdbv3api import TMDb
from datetime import datetime
import pytz
import boto3
import json
import os
import requests

# Configurar API do TMDB
tmdb = TMDb()
tmdb.api_key = os.environ.get('TMDB_API_KEY')

# Configurar fuso horário
fuso_horario = pytz.timezone('America/Sao_Paulo')

# Nome do bucket no S3
nome_bucket = 'desafio-final.data-lake'

# URLs base do TMDB
url_tmdb_filme = "https://api.themoviedb.org/3/discover/movie?include_adult=false"
url_tmdb_serie = "https://api.themoviedb.org/3/discover/tv?include_adult=false"

# Criar sessão para otimizar requisições
session = requests.Session()

# Gerar automaticamente as décadas de 1890 até o ano atual
ano_atual = datetime.now().year
decadas = [(ano, min(ano + 9, ano_atual)) for ano in range(1890, ano_atual + 1, 10)]

# Lista de gêneros (mesma para filmes e séries)
generos = "27|53|9648"  # Terror, Thriller, Mistério

# Função para buscar filmes por década
def buscar_filmes_por_decada(generos, inicio_decada, fim_decada, url):
    dados = []
    pagina_atual = 1

    print(f"Buscando filmes de {inicio_decada} até {fim_decada}...") 

    while True:
        parametros = {
            "api_key": tmdb.api_key,
            "language": "pt-BR",
            "sort_by": "popularity.desc",
            "with_genres": generos,
            "primary_release_date.gte": f"{inicio_decada}-01-01",
            "primary_release_date.lte": f"{fim_decada}-12-31",
            "page": pagina_atual
        }

        response = session.get(url, params=parametros)

        if response.status_code == 200:
            resposta_json = response.json()
            pagina_resultados = resposta_json.get("results", [])
            dados += pagina_resultados
            total_paginas = resposta_json.get("total_pages", 0)

            if pagina_atual >= total_paginas or pagina_atual >= 500:
                break
            pagina_atual += 1
        else:
            print(f"Erro na década {inicio_decada}-{fim_decada}, página {pagina_atual}: {response.status_code}, {response.text}")
            break

    return dados

# Função para buscar TODAS as séries de uma vez (sem dividir por década)
def buscar_todas_series(generos, url):
    dados = []
    pagina_atual = 1

    print("Buscando todas as séries...")  
    
    while True:
        parametros = {
            "api_key": tmdb.api_key,
            "language": "pt-BR",
            "sort_by": "popularity.desc",
            "with_genres": generos,
            "page": pagina_atual
        }

        response = session.get(url, params=parametros)

        if response.status_code == 200:
            resposta_json = response.json()
            pagina_resultados = resposta_json.get("results", [])
            dados += pagina_resultados
            total_paginas = resposta_json.get("total_pages", 0)

            if pagina_atual >= total_paginas or pagina_atual >= 500:
                break
            pagina_atual += 1
        else:
            print(f"Erro ao buscar séries, página {pagina_atual}: {response.status_code}, {response.text}")
            break

    return dados

# Buscar filmes por década
dados_filmes = []
for inicio, fim in decadas:
    filmes_decada = buscar_filmes_por_decada(generos, inicio, fim, url_tmdb_filme)
    dados_filmes.extend(filmes_decada)

# Buscar todas as séries de uma vez
dados_series = buscar_todas_series(generos, url_tmdb_serie)

# Função para salvar JSON no S3
def subirs3_arquivos_json(dados, prefixo, nome_bucket, max_por_arquivo=100):
    s3_client = boto3.client('s3')
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    arquivos_criados = 0

    for i in range(0, len(dados), max_por_arquivo):
        bloco = dados[i:i + max_por_arquivo]
        arquivo_nome = f'{prefixo}_{i // max_por_arquivo + 1}.json'
        caminho_s3 = f'RAW Zone/TMDB/JSON/{prefixo}/{ano}/{mes}/{dia}/{arquivo_nome}'
        arquivo_conteudo = json.dumps(bloco, ensure_ascii=False, indent=4)
        
        s3_client.put_object(Bucket=nome_bucket, Key=caminho_s3, Body=arquivo_conteudo, ContentType='application/json')
        arquivos_criados += 1

    return arquivos_criados

# Função principal do Lambda
def lambda_handler(event, context):
    # Enviar os arquivos para o S3
    arquivos_filmes = subirs3_arquivos_json(dados_filmes, 'filmes', nome_bucket)
    arquivos_series = subirs3_arquivos_json(dados_series, 'series', nome_bucket)

    return {
        'statusCode': 200,
        'body': f"Salvos {len(dados_filmes)} registros em {arquivos_filmes} arquivos JSON e {len(dados_series)} registros em {arquivos_series} arquivos JSON."
    }
