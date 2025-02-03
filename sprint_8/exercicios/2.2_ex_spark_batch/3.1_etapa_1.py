# ETAPA 1 - Criar uma lista de 250 números inteiros aleatórios, reverter a lista e imprimi-la.

# Importar as bibliotecas necessárias
import random

# Criar uma lista de 250 números inteiros aleatórios
lista_numeros = [random.randint(0, 1000) for i in range(250)]

# Reverter a lista
lista_numeros.reverse()

# Imprimir o resultado
print("Lista revertida:")
print(lista_numeros)