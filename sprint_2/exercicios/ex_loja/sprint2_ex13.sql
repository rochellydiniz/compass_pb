--- Exercício de programação 13: E13



SELECT 
	cdpro 
,	nmcanalvendas 
,	nmpro 
,	SUM(qtd) as quantidade_vendas 
FROM tbvendas t 
WHERE status = 'Concluído' and (cdcanalvendas = '1' OR cdcanalvendas = '2')
GROUP BY 
	cdpro 
,	nmcanalvendas 
,	nmpro 
ORDER BY quantidade_vendas ASC 
LIMIT 10