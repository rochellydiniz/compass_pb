# Dataset: Centro Federal de Educação Tecnológica de Minas Gerais - CEFET-MG
# Fonte: Dados Abertos - Governo Federal
# Plano de Dados Abertos 2022-2024 / Última atualização: 14/02/2023
# Relação de todos os alunos ingressantes e formandos, por ciclo acadêmico.
# Disponível em https://dados.gov.br/dados/conjuntos-dados/12-alunos


# Importar bibliotecas necessárias
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import boto3
import os

# Carregar as variáveis da biblioteca .env
load_dotenv()

# Cadastrar as credenciais da AWS
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

# Criar um cliente S3 usando a sessão boto3
s3_client = session.client('s3')

# Configurar o S3
nome_bucket = 'desafio-dados.abertos'
nome_arquivo_s3 = 'PDA_2022-2024_1.2_Alunos_Anonimo.csv'

# Baixar e carregar o dataset em um DataFrame
try:
    obj = s3_client.get_object(Bucket=nome_bucket, Key=nome_arquivo_s3)
    dados = obj['Body'].read().decode('latin1')
    df = pd.read_csv(StringIO(dados), sep=';', skiprows=2)

except Exception as e:
    print(f"Erro ao carregar o arquivo: {e}")
    exit(1)


# Filtro utilizando operadores lógicos: Ensino superior + situação de matrícula ativa ou concluinte
df_superior_ativo = df[(df['Nível de Ensino'] == 'GRADUAÇÃO') & ((df['Situação de Matrícula'] == 'ATIVO') | (df['Ingressante/Concluinte'] == 'Concluinte'))]


# Função de Data: calcular o tempo de conclusão do curso
def calcular_tempo_conclusao(data_ingresso, data_conclusao):

    # Tratamento de Dados: Considerar valores com mês 0 como mês 1 (janeiro)
    if data_ingresso.endswith('-0'):
        data_ingresso = data_ingresso[:-2] + '-1'
    if data_conclusao.endswith('-0'):
        data_conclusao = data_conclusao[:-2] + '-1'

    # Função de Conversão: Converter strings em objetos datetime
    data_ingresso = datetime.strptime(data_ingresso, '%Y-%m')
    data_conclusao = datetime.strptime(data_conclusao, '%Y-%m')
    return (data_conclusao.year - data_ingresso.year) * 12 + data_conclusao.month - data_ingresso.month


# Função Condicional: Aplicar apenas para concluintes
df_superior_ativo['Tempo de Estimado de Conclusão (anos)'] = df_superior_ativo.apply(
    lambda row: calcular_tempo_conclusao(row['Situação de Matrícula'], 
    row['Período de Ingresso']) / 12 
        if row['Ingressante/Concluinte'] == 'Concluinte' else None, axis=1
)


# Funções de Agregação: Agrupar e agregar dados
df_grupo = df_superior_ativo.groupby(["Ingressante/Concluinte", 'Curso']).agg({
    'Tempo de Estimado de Conclusão (anos)': lambda x: round(x.mean(), 1),
    'Nome': 'count'
}).reset_index()

# Renomear colunas agregadas
df_grupo.rename(columns={'Tempo de Estimado de Conclusão (anos)': 'Tempo Médio de Conclusão (anos)', 'Nome': 'Contagem de Alunos'}, inplace=True)
df_grupo.sort_values(by='Contagem de Alunos', ascending=False, inplace=True)


# Filtrar para obter apenas a maior contagem de alunos para ingressante e para concluinte
df_maior_contagem = df_grupo.loc[df_grupo.groupby('Ingressante/Concluinte')['Contagem de Alunos'].idxmax()]

# Função de String: Converter os dados da coluna 'Curso' para manter apenas a primeira letra maiúscula
df_maior_contagem['Curso'] = df_maior_contagem['Curso'].str.capitalize()


# Extrair informações para o resultado final
curso_max_ingres = df_maior_contagem['Curso'].iloc[1]
cont_max_ingres = df_maior_contagem['Contagem de Alunos'].iloc[1]
curso_max_conc = df_maior_contagem['Curso'].iloc[0]
cont_max_conc = df_maior_contagem['Contagem de Alunos'].iloc[0]
tempo_conclusao = df_maior_contagem['Tempo Médio de Conclusão (anos)'].iloc[0]


# Resultado final: A princípio, pensei em manter apenas a resposta dos alunos concluíntes por ter um maior número de dados.
# Porém, apesar de termos duas informações, em minha análise, acredito que fez sentido apresentar 
# uma resposta única para cada tipo de aluno, mantendo assim, sua exclusividade e relevância.
print(f'O curso com a maior número de ingressantes é {curso_max_ingres} com {cont_max_ingres} alunos.')
print(f'O curso com o maior numero de concluintes é {curso_max_conc} com {cont_max_conc} alunos e um tempo médio de conclusão de {tempo_conclusao} anos.')

# Salvar o resultado final em um arquivo CSV
df_maior_contagem.to_csv('resultado_desafio.csv', index=False)
print('Resultado salvo em "resultado_desafio.csv".')

#
filename_local = 'resultado_desafio.csv'
filename_s3 = filename_local

try:
    print(f"Subindo para o bucket S3 '{nome_bucket}'...")
    s3_client.upload_file('resultado_desafio.csv', nome_bucket, 'resultado_desafio.csv')
    print(f"Envio concluído. Arquivo salvo como '{filename_s3}' no bucket '{nome_bucket}'.")

except boto3.exceptions.S3UploadFailedError as e:
    print(f"Erro ao subir arquivo para o S3: {e}")
