#!/bin/bash

# 4.3 Criar novo relatório

# Unir todos os relatórios gerados e gerar o relatório final.

cat /home/rochelly/ecommerce/vendas/backup/relatorio-20241023.txt >> /home/rochelly/ecommerce/relatorio_final.txt
echo "" >> /home/rochelly/ecommerce/relatorio_final.txt

cat /home/rochelly/ecommerce/vendas/backup/relatorio-20241024.txt >> /home/rochelly/ecommerce/relatorio_final.txt
echo "" >> /home/rochelly/ecommerce/relatorio_final.txt

cat /home/rochelly/ecommerce/vendas/backup/relatorio-20241025.txt >> /home/rochelly/ecommerce/relatorio_final.txt
echo "" >> /home/rochelly/ecommerce/relatorio_final.txt

cat /home/rochelly/ecommerce/vendas/backup/relatorio-20241026.txt >> /home/rochelly/ecommerce/relatorio_final.txt
echo "" >> /home/rochelly/ecommerce/relatorio_final.txt

