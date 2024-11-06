--- Exercício de programação 16: E16


SELECT 
	estado 
,	nmpro 
,	ROUND( AVG(qtd),4) as quantidade_media 
FROM tbvendas t
WHERE status = 'Concluído'
GROUP BY 
	estado 
,	nmpro
ORDER BY estado, nmpro 







