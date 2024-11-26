
# Exercício ETL - Etapa 4

# # Etapa 4
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

# Dicionário para contar a ocorrência dos filmes
contador_filmes = {}

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

    # Contagem de filmes
    filme = linha_dict['#1 Movie'].strip()
    if filme in contador_filmes:
        contador_filmes[filme] += 1
    else:
        contador_filmes[filme] = 1

# Ordenação dos filmes pela frequência e nome do filme
filmes_ordenados = sorted(contador_filmes.items(), key=lambda item: (-item[1], item[0]))

# Escrever o resultado em um arquivo
with open ('etapa-4.txt', 'w') as arquivo_saida:
    for i, (filme, quantidade) in enumerate(filmes_ordenados, start=1):
        print(f'{i} - O filme {filme} aparece {quantidade} vez(es) no dataset', file=arquivo_saida)
