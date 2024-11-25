# Exercícios Parte 1 - EX01

'''
Desenvolva um código em Python que crie variáveis para armazenar o nome e a idade de uma pessoa, 
juntamente com seus valores correspondentes. 
Como saída, imprima o ano em que a pessoa completará 100 anos de idade.
'''

from datetime import datetime

nome = input ('Olá! Qual o seu nome? ')
idade = input ('Legal! E quantos anos você tem? ')
resultado =  str(datetime.now().year + (100 - int(idade)))


print('Hmmm.. Pelos meus cálculos, ' + nome + ', você completará 100 anos em ' + resultado)

# Neste exercício, utilizando o VSC, pude utilizar o comando imput para iteração com o usuário.
# Como na plataforma da Udemy não permite essa iteração, lá foi necessário determinar as variáveis sem iteragir com o usuário.
