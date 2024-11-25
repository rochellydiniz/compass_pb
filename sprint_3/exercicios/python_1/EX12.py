
# Exercícios Parte 2 - EX12

'''
Implemente a função my_map(list, f) que recebe uma lista como primeiro argumento e uma função como segundo argumento. 
Esta função aplica a função recebida para cada elemento da lista recebida e retorna o resultado em uma nova lista.
Teste sua função com a lista de entrada [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] e com uma função que potência de 2 para cada elemento.
'''

lista_entrada = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def my_map(list, f):
    return [f(x) for x in list]


def potencia(n):
    return n ** 2

lista_saida = my_map(lista_entrada, potencia)

print(lista_saida)