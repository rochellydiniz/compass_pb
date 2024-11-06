--- Exercício de programação 6: E06

SELECT
	a.codautor 
,	a.nome 
,	COUNT(l.autor) AS quantidade_publicacoes 
FROM autor a 
LEFT JOIN livro l ON a.codautor = l.autor 
GROUP BY 
	a.codautor 
,	a.nome
ORDER BY quantidade_publicacoes DESC
LIMIT 1
