
# Exercício ETL - Etapa 5

# Leitura do Arquivo
with open(r"actors.csv", 'r') as arquivo:
    dados = arquivo.readlines()

# Quebra de Colunas
delimitador = ','
qualificador = '"'

# Extrair o cabeçalho
cabecalho = dados[0].rstrip().split(delimitador)
dados = dados[1:]  # Remover o cabeçalho da lista de dados

# Inicializar dicionário para armazenar a soma da bilheteria por ator
bilheteria_por_ator = {}

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

    # Soma a bilheteria por ator
    ator = linha_dict['Actor'].strip()
    gross = float(linha_dict['Total Gross'].replace('$', '').replace(',', '').strip())
    if ator in bilheteria_por_ator:
        bilheteria_por_ator[ator] += gross
    else:
        bilheteria_por_ator[ator] = gross

# Ordenar atores pela bilheteria total de maneira decrescente
atores_ordenados = sorted(bilheteria_por_ator.items(), key=lambda x: x[1], reverse=True)

# Escrever no arquivo
with open(r"etapa-5.txt", 'w') as arquivo_saida:
    for ator, total in atores_ordenados:
        arquivo_saida.write(f"{ator} - {total:,.2f}\n")