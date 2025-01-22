
#Importando bibliotecas
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
url_base = f'https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&language=pt-BR'

# Requisição da API
response = requests.get(url_base)
data=response.json()

# Lista para armazenar os dados
filmes=[]

# Loop para extrair os dados
for movie in data['results']:
    df={
        'Título':movie['title'],
        'Data de Lançamento':movie['release_date'],
        'Visão Geral':movie['overview'],
        'Votos':movie['vote_count'],
        'Média de Votos':movie['vote_average']
    }

    filmes.append(df)


# Criar um DataFrame
df=pd.DataFrame(filmes)
display(df)

# Salvar o DataFrame em um arquivo CSV
df.to_csv('apitmdb_testeudemy.csv',index=False, encoding='utf-8-sig')
