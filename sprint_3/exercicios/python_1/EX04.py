
# Exercícios Parte 1 - EX04

'''
Escreva um código Python para imprimir todos os números primos entre 1 até 100. Lembre-se que você deverá desenvolver o cálculo que identifica se um número é primo ou não.
Importante: Aplique a função range() em seu código.
'''

# Os números são primos?

for numero in range(2,101):
    if all (numero % div != 0 
        for div in range(2, int(numero**0.5) + 1)):
        print(numero)
