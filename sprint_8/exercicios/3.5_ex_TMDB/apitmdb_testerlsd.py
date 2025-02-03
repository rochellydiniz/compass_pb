
#Importando bibliotecas
from unittest import result
import requests
import pandas as pd
from IPython.display import display
from dotenv import load_dotenv
import os

# Carregar variáveis da biblioteca .env
load_dotenv()

# Cadastrar a chave da API
api_key=os.getenv('TMDB_API_KEY')

# URL base para a API
url_base = f'https://api.themoviedb.org/3/tv/top_rated?api_key={api_key}&language=pt-BR'

# Requisição da API
response = requests.get(url_base)
data=response.json()

# Lista para armazenar os dados
series=[]

# Loop para extrair os dados
for serie in data['results']:
    df={
        'Título':serie['name'],
        'Data de Lançamento':serie['first_air_date'],
        'Visão Geral':serie['overview'],
        'Votos':serie['vote_count'],
        'Média de Votos':serie['vote_average']
    }

    series.append(df)


# Criar um DataFrame
df=pd.DataFrame(series)
display(df)

# Salvar o DataFrame em um arquivo CSV
df.to_csv('apitmdb_testerlsd.csv',index=False, encoding='utf-8-sig')
