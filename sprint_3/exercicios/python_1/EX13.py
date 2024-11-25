
# Exercícios Parte 2 - EX13

'''
Escreva um programa que lê o conteúdo do arquivo texto arquivo_texto.txt e imprime o seu conteúdo.
Dica: leia a documentação da função open(...)
'''

larquivo = open('arquivo_texto.txt','r')
conteudo = arquivo.read()

print (conteudo, end='')