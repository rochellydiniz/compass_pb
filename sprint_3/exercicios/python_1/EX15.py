
# Exercícios Parte 2 - EX15

'''
Implemente a classe Lampada. A classe Lâmpada recebe um booleano no seu construtor, 
True se a lâmpada estiver ligada, False caso esteja desligada. 
A classe Lampada possuí os seguintes métodos:
liga(): muda o estado da lâmpada para ligada
desliga(): muda o estado da lâmpada para desligada
esta_ligada(): retorna verdadeiro se a lâmpada estiver ligada, falso caso contrário
Para testar sua classe:
Ligue a Lampada
Imprima: A lâmpada está ligada? True
Desligue a Lampada
Imprima: A lâmpada ainda está ligada? False
'''

# criar a classe
class Lampada:
    
    # construtor
    def __init__(self, ligada):
        self.ligada = ligada

    # metodos
    def liga(self):
        self.ligada = True

    def desliga(self):
        self.ligada = False

    def esta_ligada(self):
        return self.ligada

# testar a classe iniciando com a lampada desligada
lampada = Lampada(False)

lampada.liga()
print(f'A lâmpada está ligada? {lampada.esta_ligada()}')

lampada.desliga()
print(f'A lâmpada ainda está ligada? {lampada.esta_ligada()}')
