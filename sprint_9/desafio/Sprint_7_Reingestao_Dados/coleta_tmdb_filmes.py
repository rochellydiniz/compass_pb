import requests
import json
import os
import pytz
from datetime import datetime
from tmdbv3api import TMDb
import boto3
import time


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
s3_client = boto3.client('s3')

# Função para rodar no AWS Lambda
def lambda_handler(event, context):
    coleta_series_genero(max_paginas=500, max_por_arquivo=100)
    return {
        'statusCode': 200,
        'body': json.dumps('Coleta de series finalizada e arquivos enviados para o bucket!')
    }


# Função para coletar series de um gênero específico
def coleta_series_genero(max_paginas=500, max_por_arquivo=100):
    series = []
    total_paginas_disponiveis = 1
    
    for pagina in range(1, max_paginas + 1):
       url = f"{url_base}/discover/tv?api_key={api_key}&include_adult=false&include_null_first_air_dates=false&with_genres={generos}&language={idioma}&page={pagina}"
       response = requests.get(url)
        
       if response.status_code == 200:
            dados = response.json()
            total_paginas_disponiveis = dados.get("total_pages", 1)
            series.extend(dados.get('results', [])) 

            print(f"Página {pagina}/{total_paginas_disponiveis} coletou ({len(dados.get('results',[]))} series)!")       
            
            if pagina >= total_paginas_disponiveis:
                print(f"Todas as {total_paginas_disponiveis} páginas foram coletadas.")
                break
            
       else:
            print(f"Erro ao coletar página {pagina}! Status: {response.status_code}")
            break
        

    # Criar estrutura do S3
    agora = datetime.now(fuso_horario)
    ano, mes, dia = agora.strftime('%Y'), agora.strftime('%m'), agora.strftime('%d')
    prefixo_s3 = f"RAW Zone/TMDB/JSON/series/{ano}/{mes}/{dia}/"

    arquivos_criados = 0

    caminho_s3 = None
     
    # Dividir os dados em arquivos de 100 series
    for i in range(0, len(series), max_por_arquivo):
        bloco = series[i:i + max_por_arquivo]
        arquivo_nome = f'series_{i // max_por_arquivo + 1}.json'
        caminho_s3 = f"{prefixo_s3}{arquivo_nome}"

        # Subir para o bucket
        s3_client.put_object(
            Bucket=nome_bucket,
            Key=caminho_s3,
            Body=json.dumps(bloco, indent=4, ensure_ascii=False),
            ContentType='application/json'
        )

        print(f"Arquivo {arquivo_nome} enviado para o bucket!")
        arquivos_criados += 1


    print(f"{len(series)} séries salvas em {arquivos_criados} arquivos no s3: {caminho_s3}!")
