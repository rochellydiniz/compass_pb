def conta_vogais(texto:str)-> int:
    vogais = 'AEIOUaeiou'

    
    vogais_filtro = filter (lambda x: x in vogais, texto)
    return len(list(vogais_filtro))


texto = 'O rato roeu a roupa do rei de Roma.'    
print (conta_vogais(texto))