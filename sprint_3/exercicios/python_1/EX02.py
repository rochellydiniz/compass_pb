
# Exercícios Parte 1 - EX02

'''
Escreva um código Python que use a função range() para adicionar três números em uma lista
(Esta lista deve chamar-se 'números') e verificar se esses três números são pares ou ímpares. 
Para cada número, imprima como saída Par: ou Ímpar: e o número correspondente (um linha para cada número lido).
Importante: Aplique a função range() em seu código.
'''

# Adicionar três números em uma lista chamada 'numeros'

numeros = list(range(92,95))


# Os número são pares ou ímpares?

for numero in numeros:
    if numero % 2 == 0:
        print(f"Par: {numero}")
    else:
        print(f"Ímpar: {numero}")