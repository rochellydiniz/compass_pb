
def calcular_valor_maximo(operadores,operandos) -> float:
    calculos = map(
            lambda operacao:    operacao[1][0] + operacao[1][1] if operacao[0] == '+' else
                                operacao[1][0] - operacao[1][1] if operacao[0] == '-' else
                                operacao[1][0] * operacao[1][1] if operacao[0] == '*' else
                                operacao[1][0] / operacao[1][1] if operacao[0] == '/' else
                                operacao[1][0] % operacao[1][1],
            zip(operadores, operandos)
        )
    
    return max(calculos)
    
    
operadores = ['+','/','%','+','*','-','-']
operandos = [(10,78),(1301,46),(91,5),(-654,87.6),(165,29),(-62.84,32),(-9856,-541)]
print(calcular_valor_maximo(operadores,operandos))