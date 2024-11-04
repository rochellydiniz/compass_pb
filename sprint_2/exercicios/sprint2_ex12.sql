--- Exercício de programação 12: E12



SELECT 
	dep.cddep 
,	dep.nmdep 
,	dep.dtnasc
, 	SUM(ven.qtd*ven.vrunt) as valor_total_vendas
FROM tbdependente dep 
LEFT JOIN tbvendas ven on dep.cdvdd = ven.cdvdd 
WHERE ven.status = 'Concluído'
GROUP BY 
	dep.cddep 
,	dep.nmdep 
,	dep.dtnasc 
ORDER BY valor_total_vendas ASC 
LIMIT 1

