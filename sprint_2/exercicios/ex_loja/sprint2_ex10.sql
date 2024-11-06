--- Exercício de programação 10: E10


SELECT 
	vdd.nmvdd as vendedor
,	SUM(ven.qtd * ven.vrunt) as valor_total_vendas
,	ROUND( SUM((vdd.perccomissao * ven.qtd * ven.vrunt))/100,2) as comissao
FROM tbvendedor vdd
LEFT JOIN tbvendas ven on vdd.cdvdd = ven.cdvdd 
WHERE ven.status = 'Concluído'
GROUP BY vdd.nmvdd 
ORDER BY comissao DESC 






