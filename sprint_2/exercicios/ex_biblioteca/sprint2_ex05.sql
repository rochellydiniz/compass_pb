--- Exercício de programação 5: E05


SELECT DISTINCT 
	a.nome  
FROM autor a 
LEFT JOIN livro l ON a.codautor = l.autor 
LEFT JOIN editora e ON l.editora = e.codeditora 
LEFT JOIN endereco e2 on e.endereco = e2.codendereco 
WHERE e2.estado NOT IN ('RIO GRANDE DO SUL', 'SANTA CATARINA', 'PARANÁ')
ORDER BY a.nome 

