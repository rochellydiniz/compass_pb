#!/bin/bash

#Desafio - Sprint 1

# 4.1: Criar um diretório chamado vendas e copiar o arquivo dados_de_vendas.csv
mkdir /home/rochelly/ecommerce/vendas
cp /home/rochelly/ecommerce/dados_de_vendas.csv /home/rochelly/ecommerce/vendas/


# Criar um subdiretório chamado backup e copiar o arquivo dados_de_vendas com a data de execução como parte do nome do arquivo
mkdir /home/rochelly/ecommerce/vendas/backup
cp /home/rochelly/ecommerce/vendas/dados_de_vendas.csv /home/rochelly/ecommerce/vendas/backup/dados-$(date '+%Y%m%d').csv


# Dentro de backup, renomear o arquivo
cd /home/rochelly/ecommerce/vendas/backup
mv dados-$(date '+%Y%m%d').csv backup-dados-$(date '+%Y%m%d').csv


# Dentro de backup, criar arquivo relatorio.txt contendo as seguintes informações
touch /home/rochelly/ecommerce/vendas/backup/relatorio-$(date '+%Y%m%d').txt

# Data do Sistema Operacional
date '+%Y/%m/%d %H:%M' >> relatorio-$(date '+%Y%m%d').txt

# Data do primeiro registro de vendas
head -n 2 backup-dados-*.csv | tail -n 1 | cut -d',' -f5 >> relatorio-$(date '+%Y%m%d').txt

# Data do último registro de vendas
tail -n 1 backup-dados-*.csv | cut -d',' -f5 >> relatorio-$(date '+%Y%m%d').txt

# Quantidade total de itens diferentes vendidos
cut -d',' -f2 backup-dados-*.csv | tail -n +2 | uniq | wc -l >> relatorio-$(date '+%Y%m%d').txt

# Primeiras 10 linhas do arquivo
head -n 11 backup-dados-*.csv >> relatorio-$(date '+%Y%m%d').txt


# Comprimir o arquivo
zip backup-dados-$(date '+%Y%m%d').zip backup-dados-*.csv


# Apagar os arquivos backup-dados*.csv e dados_de_vendas.csv de seus respectivos diretórios
rm /home/rochelly/ecommerce/vendas/dados_de_vendas.csv /home/rochelly/ecommerce/vendas/backup/backup-dados-*.csv 



