
# Exercícios Parte 2 - EX17

'''
Escreva uma função que recebe como parâmetro uma lista e retorna 3 listas: 
a lista recebida dividida em 3 partes iguais. 
Teste sua implementação com a lista abaixo
lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
'''

lista0 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

def dividir_lista(lista0):
    total = len(lista0) // 3

    lista1 = lista0[:total]
    lista2 = lista0[total:2 * total]
    lista3 = lista0[2 * total:]
    
    return lista1, lista2, lista3

lista1, lista2, lista3 = dividir_lista(lista0)

print(f'{lista1} {lista2} {lista3}')