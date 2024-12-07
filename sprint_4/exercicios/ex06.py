def maiores_que_media(conteudo:dict)->list:
    media = sum(conteudo.values()) / len (conteudo)
    prod_acima_media = [(produto, preco) for produto, preco in conteudo.items() if preco > media]
    prod_sort = sorted(prod_acima_media, key=lambda x: x[1])
    return prod_sort
    
conteudo = {
    "hamburguer bovino": 18.89,
    "alface americana": 3.49,
    "queijo tipo cheddar": 6.99,
    "molho Billy Jack": 29.90,
    "cebola": 4.59,
    "pepino em conserva": 25.29,
    "pão para hamburguer c/ gergelim": 11.90,
    "batata palito congelada": 30.89,
    "refrigerante tipo cola 2L": 10.99
}    

resultado = maiores_que_media(conteudo)
print (resultado)