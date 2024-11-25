
# Exercícios Parte 2 - EX07

'''
Dada a seguinte lista:
a = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
Faça um programa que gere uma nova lista contendo apenas números ímpares.
'''

a = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
impares = []

for numero in a:
    if numero % 2 == 1:
        impares.append(numero)

print(impares)