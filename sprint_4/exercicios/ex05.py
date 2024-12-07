import csv


def relatorio_notas(boletim):
    with open(boletim, 'r') as file:
        consulta = csv.reader(file)
        estudantes = []
        
        for linha in consulta:
            nome = linha[0]
            notas = list(map(int, linha[1:]))
            notas_top3 = sorted(notas, reverse=True)[:3]
            media = round(sum(notas_top3) / len(notas_top3), 2)
            estudantes.append((nome, notas_top3, media))
            
    estudantes_alfab = sorted(estudantes, key=lambda x: x[0])

    for estudante in estudantes_alfab:
        nome, notas_top3, media = estudante
        print(f'Nome: {nome} Notas: {notas_top3} Média: {media}')

    
relatorio_notas('estudantes.csv')