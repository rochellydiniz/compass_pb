--- Exercício de programação 11: E11



SELECT 
	cdcli
,	nmcli
,	SUM(qtd*vrunt) as gasto
FROM tbvendas t 
WHERE status = 'Concluído'
GROUP BY 
	cdcli
,	nmcli
ORDER BY gasto DESC 
LIMIT 1
