
from functools import reduce

def calcula_saldo(lancamentos) -> float:
    valor = map(lambda x: x[0] if x[1] == 'C' else -x [0], lancamentos)
    resultado = reduce(lambda calc_acumulado, saldo: calc_acumulado + saldo, valor) 
    return resultado
    
lancamentos = [
    (750, 'C'),
    (910, 'C'),
    (225.25, 'D'),
    (0.75, 'C'),
    ]
    
print (calcula_saldo(lancamentos))