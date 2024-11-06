--- Exercício de programação 4: E04
	
SELECT
	a.nome 
,	a.codautor	
,	a.nascimento
,	count(l.cod) as quantidade
FROM autor a 
LEFT JOIN livro l ON a.codautor = l.autor 
GROUP BY 
	a.nome 
,	a.codautor	
,	a.nascimento	
ORDER BY a.nome






