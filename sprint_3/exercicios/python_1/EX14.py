
# Exercícios Parte 2 - EX14

'''
Escreva uma função que recebe um número variável de parâmetros não nomeados e um número variado de parâmetros nomeados e imprime o valor de cada parâmetro recebido.
Teste sua função com os seguintes parâmetros:
(1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)
'''

def params (*args, **kwargs):
    for dado in args:
        print(dado)

    for dado in kwargs.values():
        print(dado)


params (1, 3, 4, 'hello', parametro_nomeado='alguma coisa', x=20)