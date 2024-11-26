
# # Exercício ETL - etapa 1

# Leitura do Arquivo
with open(r"actors.csv", 'r') as arquivo:
    dados = arquivo.readlines()

# Quebra de Colunas
delimitador = ','
qualificador = '"'

# Extrair o cabeçalho
cabecalho = dados[0].rstrip().split(delimitador)
dados = dados[1:]  # Remover o cabeçalho da lista de dados

# Inicializar lista para armazenar os dicionários
dados_formatados = []

# Percorre Linhas
for linha in dados:
    qualificadorAtivo = False
    texto = ''
    colunas = []

    # Percorre Caracteres
    for letra in linha.rstrip():
        # Ativa ou desativa informação se o caractere está dentro de um qualificador
        if letra == qualificador and not qualificadorAtivo:
            qualificadorAtivo = True
        elif letra == qualificador and qualificadorAtivo:
            qualificadorAtivo = False

        # Verifica se o caractere é o delimitador e se não está dentro de um qualificador, para quebrar em colunas
        if letra == delimitador and not qualificadorAtivo:
            colunas.append(texto.replace(qualificador, ''))
            texto = ''
        else:
            texto += letra

    # Adiciona a última coluna após o loop
    if texto:
        colunas.append(texto.replace(qualificador, ''))

    # Cria um dicionário combinando o cabeçalho com as colunas
    linha_dict = dict(zip(cabecalho, colunas))
    dados_formatados.append(linha_dict)

# Encontrar o ator/atriz com maior número de filmes
max_filmes = 0
ator_max_filmes = ""

for registro in dados_formatados:
    num_filmes = int(registro['Number of Movies'].strip())
    if num_filmes > max_filmes:
        max_filmes = num_filmes
        ator_max_filmes = registro['Actor']

# Escrever o resultado em um arquivo
with open ('etapa-1.txt', 'w') as arquivo_saida:
    print(f' Ator com mais filmes: {ator_max_filmes} / Quantidade de filmes: {max_filmes}', file=arquivo_saida)
