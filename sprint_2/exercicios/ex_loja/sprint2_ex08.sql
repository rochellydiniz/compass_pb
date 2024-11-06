--- Exercício de programação 8: E08


SELECT
	TPROF.cdvdd 
,	TPROF.nmvdd 
FROM tbvendedor tprof  
LEFT JOIN tbvendas tven ON TPROF.cdvdd = TVEN.cdvdd
WHERE TVEN.status = 'Concluído'
GROUP BY 
	TPROF.cdvdd 
,	TPROF.nmvdd
ORDER BY COUNT(TVEN.cdven) DESC
LIMIT 1

