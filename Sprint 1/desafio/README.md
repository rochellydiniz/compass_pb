# Desafio - Sprint 1

## Objetivo


Praticar comandos Markdown e SO Linux.

<br>

## Descrição

Através dos dados de vendas de uma loja online em arquivo .csv, criar um arquivo executável que realiza tarefas usando comandos do Linux, gerando um relatório com informações específicas.     
Agendar para que este script seja executado todos os dias, de segunda a quinta-feira às 15h27.   
Ao final dos quatro dias, criar um script consolidador para unir os relatórios gerados em um único arquivo.

<br>

## Etapas

<br> 

### 3. Preparação

* Fazer o download do arquivo dados_de_vendas.csv.

![Evidência 1](/Sprint%201/evidencias/evid_desafio/etapa3/01.jpg)

_Evidência 1: Arquivo dados_de_vendas na pasta de downloads no ambiente Linux_              
<br>

* No Linux, criar o diretório ecommerce.             

![Evidência 2](/Sprint%201/evidencias/evid_desafio/etapa3/02.jpg)
_Evidência 2: Código mkdir para criar a pasta ecommerce._                        
<br>

* Mover o arquivo dados_de_vendas.csv para o diretório ecommerce.

![Evidência 3](/Sprint%201/evidencias/evid_desafio/etapa3/03.jpg)
_Evidência 3: Código mv para mover o arquivo para outra pasta. Código cd para entrar dentro do diretório ecommerce_

<br>

### 4.1 Criar um arquivo executável

* Criar um arquivo executável chamado processamento_de_vendas.sh que realiza as tarefas abaixo através de comandos Linux.

![Evidência 4](/Sprint%201/evidencias/evid_desafio/etapa4-1/04.jpg)
_Evidência 4: Comando touch para criar um arquivo vazio, finalizando com extensão em .sh para determinar que é um arquivo executável. Comando sudo apt get install nano, para instalar o editor de texto Nano._                               

<br>

* Criar um diretório chamado vendas.

![Evidência 5](/Sprint%201/evidencias/evid_desafio/etapa4-1/05.jpg)
_Evidência 5: Comando mkdir para criar o diretório vendas_

<br>

* Copiar o arquivo dados_de_vendas.csv para esta pasta.

![Evidência 5](/Sprint%201/evidencias/evid_desafio/etapa4-1/05.jpg)
_Evidência 5: Comando cp para copiar o arquivo do diretório ecommerce para o diretorio ecommerce/vendas._

<br>

* Criar o subdiretório backup dentro do diretório vendas e copiar para o backup o arquivo dados_de_vendas.csv acrescentando no nome do arquivo a data de execução precedida de hífen.

![Evidência 6](/Sprint%201/evidencias/evid_desafio/etapa4-1/06.jpg)
_Evidência 6: Comando mkdir para criar o subdiretório backup. Comando cp para copiar o arquivo dados_de_vendas.csv do diretório vendas *e* para o diretório backup com novo nome._

<br>

* No diretório backup, renomear o arquivo dados-yyyymmdd.csv para backup-dados-yyyymmdd.csv.

![Evidência 7](/Sprint%201/evidencias/evid_desafio/etapa4-1/07.jpg)
_Evidência 7: Comando cd para entrar na pasta backup. Comando mv para alterar o nome do arquivo._

<br>

* No diretório backup, criar um arquivo chamado relatorio.txt que contenha as informações abaixo.

* Data do sistema operacional em formato YYYY/MM/DD HH:MM

* Data do primeiro registro de vendas.

* Data do último registro de vendas.

* A quantidade total de itens diferentes vendidos.

* As 10 primeiras linhas do arquivo backup-dados-yyyymmdd.csv.

* Comprimir o arquivo backup-dados-yyyymmdd.csv para o arquivo backup-dados-yyyymmdd.zip.

* No diretório backup, apagar o arquivo backup-dados-yyyymmdd.csv. No diretório vendas, apagar o arquivo dados_de_vendas.csv.



<br>

### 4.2 Agendar a execução do processamento

* Criar um agendamento de execução de comandos Linux para executar o arquivo processamento_de_vendas.sh todos os dias de segunda a quinta-feira às 15h27.

* Conceder permissões para execução do script.


<br>

### 4.3 Criar novo relatório

* Uma vez ao dia, modificar manual e completamente os dados do arquivo de vendas dados_de_vendas.csv que estão no diretório ecommerce.

* Certificar-se que o arquivo processamento_de_vendas.sh esteja corretamente agendado.

* Criar um script chamado consolidador_de_processamento_de_vendas.sh que une todos os relatórios gerados para um arquivo novo chamado relatorio_final.txt.

* Após deversas execuções do script processamento_de_vendas.sh, executar manualmente o script consolidador_de_processamento_de_vendas.sh