
# Exercícios Parte 2 - EX21

'''
Implemente duas classes, Pato e Pardal , que herdam de uma superclasse chamada Passaro as habilidades de voar e emitir som.
Contudo, tanto Pato quanto Pardal devem emitir sons diferentes (de maneira escrita) no console, conforme o modelo a seguir.
Imprima no console exatamente assim:
Pato
Voando...
Pato emitindo som...
Quack Quack
Pardal
Voando...
Pardal emitindo som...
Piu Piu
'''

class Passaro:
    
    def voo(self):
        print('Voando...')

    def som(self):
        pass

class Pato(Passaro):
    def som(self):
        print('Pato emitindo som...')
        print('Quack Quack')

class Pardal(Passaro):
    def som(self):
        print('Pardal emitindo som...')
        print('Piu Piu')

print('Pato')
pato = Pato()
pato.voo()
pato.som()

print('Pardal')
pardal = Pardal()
pardal.voo()
pardal.som()