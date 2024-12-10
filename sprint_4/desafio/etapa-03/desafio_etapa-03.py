
import hashlib


# Passo 4 - Retornar ao passo 1
while True:

    # Passo 1 - Receber uma string via input
    print()
    palavra = input("Digite uma palavra: ")
    
    # Passo 2 - Gerar o hash da string por meio do algoritmo SHA-1
    hash_sha = hashlib.sha1(palavra.encode()).hexdigest()

    # Passo 3 - Imprimir a hash em tela, utilizando o método hexdigest
    print (f'O hash SHA-1 da palavra {palavra} é: {hash_sha}.')
    print()

