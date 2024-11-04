--- Exercício de programação 7: E07

SELECT
	a.nome 
FROM autor a 
LEFT JOIN livro l ON a.codautor = l.autor 
WHERE l.cod ISNULL 
GROUP BY a.nome
ORDER BY a.nome ASC 
