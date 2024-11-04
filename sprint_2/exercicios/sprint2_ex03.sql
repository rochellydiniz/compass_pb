--- Exercício de programação 3: E03
	
SELECT
	count(l.cod) as quantidade
,	e.nome
,	e2.estado 
,	e2.cidade 
FROM editora e 
INNER JOIN livro l ON e.codeditora = l.editora 
LEFT JOIN endereco e2 on e.endereco = e2.codendereco 
GROUP BY 
	e.nome
,	e2.estado 
,	e2.cidade 
ORDER BY quantidade DESC
LIMIT 5













