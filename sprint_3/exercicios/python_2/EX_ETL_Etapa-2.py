
# Exercício ETL - Etapa 2


# Leitura do Arquivo
with open(r"actors.csv", 'r') as arquivo:
    dados = arquivo.readlines()

# Quebra de Colunas
delimitador = ','
qualificador = '"'

# Extrair o cabeçalho
cabecalho = dados[0].rstrip().split(delimitador)
dados = dados[1:]  # Remover o cabeçalho da lista de dados

# Inicializar variável para somar a bilheteria
soma_bilheteria = 0.0

# Contar o número de linhas (filmes)
num_filmes = len(dados)

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

    # Soma a bilheteria (convertendo para float e removendo espaços)
    soma_bilheteria += float(linha_dict['Gross'].strip())

# Calcula a média da bilheteria
media_bilheteria = soma_bilheteria / num_filmes if num_filmes > 0 else 0

# Exibe a média da bilheteria
with open ('etapa-2.txt', 'w') as arquivo_saida:
    print(f'Receita bruta dos principais filmes (em milhoes): ${media_bilheteria:,.2f}', file=arquivo_saida)