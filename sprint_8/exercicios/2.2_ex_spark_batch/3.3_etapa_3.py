# ETAPA 3 - Gerar um dataset de nomes de pessoas.

# Importar bibliotecas necessárias
import random
import time
import names


# Definir a semente de aleatoriedade
random.seed(40)
qtd_nomes_unicos = 3000
qtd_nomes_aleatorios = 10000000


# Iniciar o contador de tempo
start_time = time.time()


# Gerar uma lista de nomes únicos
print("Gerando lista de nomes únicos...")
nomes_unicos = [names.get_full_name() for _ in range(qtd_nomes_unicos)]


# Gerar o dataset de nomes aleatórios
print("Gerando dataset de nomes aleatórios...")
nomes_aleatorios = [random.choice(nomes_unicos) for _ in range(qtd_nomes_aleatorios)]


# Calcular o tempo de execução
end_time = time.time()
tempo_processamento = end_time - start_time


# Salvar o dataset em um txt, cada nome em uma linha
with open("nomes_aleatorios.txt", "w") as f:
    for nome in nomes_aleatorios:
        f.write(nome + "\n")


print("Dataset de nomes aleatórios gerado com sucesso!")
print(f"Tempo de processamento: {tempo_processamento:.2f} segundos.")