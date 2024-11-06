--- Exercício de programação 9: E09



SELECT 
	cdpro 
,	nmpro
FROM tbvendas
WHERE dtven BETWEEN '2014-02-03' AND '2018-02-02' 
	AND status = 'Concluído'
GROUP BY 	
	cdpro 
,	nmpro
ORDER BY COUNT(cdpro) DESC
LIMIT 1


