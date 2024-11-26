
# Exercício ETL - Etapa 3

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

# Inicializar variável para armazenar o ator/atriz com a maior média
max_media = 0.0
ator_max_media = ''

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

    # Verifica e atualiza o ator/atriz com a maior média de bilheteria
    media_atual = float(linha_dict['Average per Movie'].strip())
    if media_atual > max_media:
        max_media = media_atual
        ator_max_media = linha_dict['Actor']

# Exibe o ator ou atriz com a maior média de bilheteria por filme
with open ('etapa-3.txt', 'w') as arquivo_saida:
    print(f'O ator ou atriz com a maior media de receita de bilheteria por filme e: {ator_max_media} com uma media de {max_media:.2f}', file=arquivo_saida)