# ETAPA 2 - Trabalhar com uma lista de nomes de 20 animais, ordená-los, salvar em um arquivo CSV e imprimi-los.

# Criar uma lista com 20 animais
animais = ["cachorro", "gato", "papagaio", "peixe", "leão", 
           "tigre", "elefante", "girafa", "macaco", "pato",
           "galinha", "vaca", "boi", "cavalo", "zebra",
           "cobra", "lagarto", "sapo", "rato", "coelho"]


# Ordenar a lista em ordem alfabética
animais.sort()

# Imprimir a lista usando list comprehension
print(f"Lista: {[animal for animal in animais]}")

# Salvar a lista em um arquivo CSV, um item por linha
with open("3.2_etapa_2_animais.csv", "w") as arquivo:
    for animal in animais:
        arquivo.write(f"{animal}\n")

print("Arquivo 'animais.csv' criado com sucesso!")