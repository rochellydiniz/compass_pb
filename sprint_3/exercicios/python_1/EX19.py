
# Exercícios Parte 2 - EX19

'''
Calcule o valor mínimo, valor máximo, valor médio e a mediana da lista gerada na célula abaixo:
Obs.: Lembrem-se, para calcular a mediana a lista deve estar ordenada!

import random 
# amostra aleatoriamente 50 números do intervalo 0...500
random_list = random.sample(range(500),50)

Use as variáveis abaixo para representar cada operação matemática:
mediana media valor_minimo valor_maximo 

Importante: Esperamos que você utilize as funções abaixo em seu código:
random max min sum
'''

import random

random_list = random.sample(range(500), 50)
#print(random_list)

mediana = 0
media = 0
valor_minimo = 0
valor_maximo = 0


valor_minimo = min(random_list)
valor_maximo = max(random_list)
media = sum(random_list) / len(random_list)


# mediana:
random_list.sort()
#print(random_list)

if len(random_list) % 2 == 0:
    index_mediana = len(random_list) // 2
    mediana = (random_list[index_mediana -1] + random_list[index_mediana]) / 2
else:
    mediana = random_list[len(random_list) // 2]

print(f'Valor mínimo: {valor_minimo}')
print(f'Valor máximo: {valor_maximo}')
print(f'Média: {media}')
print(f'Mediana: {mediana}')

# Acrescentei os prints (comentários) para verificar se os valores estavam corretos.