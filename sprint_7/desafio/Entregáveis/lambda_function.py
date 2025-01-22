from tmdbv3api import TMDb
from datetime import datetime
import pytz
import boto3
import json
import os
import requests


# Cadastrar credencial TMDB
tmdb = TMDb()
tmdb.api_key = os.environ['TMDB_API_KEY']


# Configurar o fuso horário
fuso_horario = pytz.timezone('America/Sao_Paulo')


# Nome do bucket no S3
nome_bucket = 'desafio-final.data-lake'


# URLs base da API do TMDB
url_tmdb_filme = 'https://api.themoviedb.org/3/discover/movie'
url_tmdb_serie = 'https://api.themoviedb.org/3/discover/tv'


# Função para buscar dados
def buscar_dados(url, parametros):
    dados = []
    pagina_atual = 1

    while True:
        parametros['page'] = pagina_atual
        response = requests.get(url, params=parametros)
        if response.status_code == 200:
            resposta_json = response.json()
            pagina_resultados = resposta_json.get('results', [])
            dados += pagina_resultados
            total_paginas = resposta_json.get('total_pages', 0)

            if pagina_atual >= total_paginas:
                break
            pagina_atual += 1
        else:
            print(f'Erro na página {pagina_atual}: {response.status_code}, {response.text}')
            break

    return dados


# Função para salvar em múltiplos arquivos JSON na S3
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

def lambda_handler(event, context):
   
   # Parâmetros filmes:
    parametros_filmes = {
        "api_key": tmdb.api_key,
        "language": "pt-BR",
        "sort_by": "popularity.desc",
        "with_genres": "27,9648",
        "page": 1
    }


    # Parâmetros séries:
    parametros_series = {
        "api_key": tmdb.api_key,
        "language": "pt-BR",
        "sort_by": "popularity.desc",
        "with_genres": "9648",
        "page": 1
    }


    # Execução
    filmes_genero = buscar_dados(url_tmdb_filme, parametros_filmes)
    series_genero = buscar_dados(url_tmdb_serie, parametros_series)


    # Salvar os resultados no S3
    arquivos_filmes = subirs3_arquivos_json(filmes_genero, 'filmes', nome_bucket)
    arquivos_series = subirs3_arquivos_json(series_genero, 'series', nome_bucket)
    
    
    return {
        'statusCode': 200,
        'body': f"Salvos {len(filmes_genero)} registros de filmes em {arquivos_filmes} arquivos JSON 
            e {len(series_genero)} registros de séries em {arquivos_series} arquivos JSON."
    }
