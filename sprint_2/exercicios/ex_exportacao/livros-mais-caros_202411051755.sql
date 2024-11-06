-- Exercício de Exportação de Dados - 3.1 Etapa 1

SELECT
	l.cod 		as CodLivro
,	l.titulo 	as Titulo
,	l.autor 	as CodAutor
,	a.nome 		as NomeAutor
,	l.valor		as Valor
,	l.editora 	as CodEditora
,	e.nome 		as NomeEditora
FROM livro l
LEFT JOIN autor a ON l.autor = a.codautor 
LEFT JOIN editora e ON l.editora = e.codeditora 
ORDER BY valor DESC 
LIMIT 10

