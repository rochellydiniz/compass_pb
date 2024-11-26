
# Exercícios Parte 2 - EX23

'''
Crie uma classe  Calculo  que contenha um método que aceita dois parâmetros, X e Y, e retorne a soma dos dois. 
Nessa mesma classe, implemente um método de subtração, que aceita dois parâmetros, X e Y, 
e retorne a subtração dos dois (resultados negativos são permitidos).
Utilize os valores abaixo para testar seu exercício:
x = 4 
y = 5
imprima:
Somando: 4+5 = 9
Subtraindo: 4-5 = -1
'''

class Calculo:
    
    def Somar(self, x, y):
        return x + y

    def Subtrair(self, x, y):
        return x - y
    

x = 4 
y = 5

resultado = Calculo()

soma = resultado.Somar(x, y)
subtracao = resultado.Subtrair(x, y)

print(f'Somando: {x}+{y} = {soma}')
print(f'Subtraindo: {x}-{y} = {subtracao}')