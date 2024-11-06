-- Exercício de Exportação de Dados - 3.2 Etapa 2

SELECT
	l.editora as CodEditora
,	e.nome as NomeEditora
,	count(l.cod) as QuantidadeLivros
FROM editora e 
INNER JOIN livro l ON e.codeditora = l.editora 
GROUP BY 
	l.editora 
,	e.nome
ORDER BY QuantidadeLivros DESC
LIMIT 5

