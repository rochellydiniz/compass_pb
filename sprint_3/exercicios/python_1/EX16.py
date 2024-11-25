
# Exercícios Parte 2 - EX16

'''
Escreva uma função que recebe uma string de números separados por vírgula e 
retorne a soma de todos eles. Depois imprima a soma dos valores.
A string deve ter valor  "1,3,4,6,10,76"
'''

numeros = "1,3,4,6,10,76"

def soma (numeros):
    list_nums = map(int, numeros.split(','))
    return sum(list_nums)

resultado = soma(numeros)

print(resultado)