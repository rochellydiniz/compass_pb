# :jigsaw: Desafio - Sprint 1

<br>

## :dart: Objetivo

Praticar comandos Markdown e SO Linux.

<br>

## :thinking: Descrição 

Através dos dados de vendas de uma loja online em arquivo .csv, criar um arquivo executável que realiza tarefas usando comandos do Linux, gerando um relatório com informações específicas.     
Agendar para que este script seja executado todos os dias, de segunda a quinta-feira às 15h27.   
Ao final dos quatro dias, criar um script consolidador para unir os relatórios gerados em um único arquivo.

<br>

## :heavy_check_mark: Etapas

<br> 

* [Preparação](#3-preparação)
* [Criar um arquivo executável](#41-criar-um-arquivo-executável)
* [Agendar a execução do processamento](#42-agendar-a-execução-do-processamento)
* [Criar novo relatório](#43-criar-novo-relatório)

<br>

### :scroll: 3. Preparação

* Fazer o download do arquivo dados_de_vendas.csv.

![Evidência 1](/sprint_1/evidencias/evid_desafio/etapa3/01.jpg)                           

_Evidência 1: Arquivo dados_de_vendas na pasta de downloads no ambiente Linux_              
<br>

* No Linux, criar o diretório ecommerce.             

![Evidência 2](/sprint_1/evidencias/evid_desafio/etapa3/02.jpg)                                   
_Evidência 2: Código mkdir para criar a pasta ecommerce._                        
<br>

* Mover o arquivo dados_de_vendas.csv para o diretório ecommerce.

![Evidência 3](/sprint_1/evidencias/evid_desafio/etapa3/03.jpg)                                      
_Evidência 3: Código mv para mover o arquivo para outra pasta. Código cd para entrar dentro do diretório ecommerce_

<br>

### 4.1 Criar um arquivo executável

* Criar um arquivo executável chamado processamento_de_vendas.sh que realiza as tarefas abaixo através de comandos Linux.

![Evidência 4](/sprint_1/evidencias/evid_desafio/etapa4-1/04.jpg)                                    
_Evidência 4: Comando touch para criar um arquivo vazio, finalizando com extensão em .sh para determinar que é um arquivo executável. Comando sudo apt get install nano, para instalar o editor de texto Nano. Para abrir o editor de texto, realizar o comando nano processamento_de_vendas.sh ._                               

<br>

* Criar um diretório chamado vendas.

![Evidência 5](/sprint_1/evidencias/evid_desafio/etapa4-1/05.jpg)                                          
_Evidência 5: Comando mkdir para criar o diretório vendas_

<br>

* Copiar o arquivo dados_de_vendas.csv para esta pasta.

![Evidência 5](/sprint_1/evidencias/evid_desafio/etapa4-1/05.jpg)                                   
_Evidência 5: Comando cp para copiar o arquivo do diretório ecommerce para o diretório ecommerce/vendas._

<br>

* Criar o subdiretório backup dentro do diretório vendas e copiar para o backup o arquivo dados_de_vendas.csv acrescentando no nome do arquivo a data de execução precedida de hífen.

![Evidência 6](/sprint_1/evidencias/evid_desafio/etapa4-1/06.jpg)                                   
_Evidência 6: Comando mkdir para criar o subdiretório backup. Comando cp para copiar o arquivo dados_de_vendas.csv do diretório vendas *e* para o diretório backup com novo nome. O comando ao final do nome do arquivo, acrescenta a data de forma dinâmica._

<br>

* No diretório backup, renomear o arquivo dados-yyyymmdd.csv para backup-dados-yyyymmdd.csv.

![Evidência 7](/sprint_1/evidencias/evid_desafio/etapa4-1/07.jpg)                             
_Evidência 7: Comando cd para entrar na pasta backup. Comando mv para alterar o nome do arquivo._

<br>

* No diretório backup, criar um arquivo chamado relatorio.txt que contenha as informações abaixo.

![Evidência 8](/sprint_1/evidencias/evid_desafio/etapa4-1/08.jpg)                                   
_Evidência 8: Comando touch para criar um arquivo .txt. Acrescentei o comando de data ao final do nome do arquivo de relatório também, para que os relatórios dos dias seguintes não sobrescrevessem os dados já gerados._

<br>

* Data do sistema operacional em formato YYYY/MM/DD HH:MM

![Evidência 9](/sprint_1/evidencias/evid_desafio/etapa4-1/09.jpg)                                                   
_Evidência 9: Comando date para informar data e hora do sistema operacional. Símbolos de maior (>>) para incluir essa informação no arquivo de relatório._

<br>

* Data do primeiro registro de vendas.

![Evidência 10](/sprint_1/evidencias/evid_desafio/etapa4-1/10.jpg)                                     
_Evidência 10: Comando head para mostrar as primeiras informações do arquivo, -n 2 significa que quero mostrar apenas as duas primeiras linhas. Pipeline ( | ) identifica que o comando anterior já terminou e que o próximo pode iniciar. Comando tail para mostrar, de acordo com o resultado da seleção anterior, as últimas linhas, -n 1 significa que quero mostrar apenas a última linha, descartando nesta situação, o cabeçalho. Comando cut vai dividir as parte desta linha, utilizando o -d"," (delimitador vírgula) e mostrar o dado da posição -f5 (quinta coluna). Símbolos de maior (>>) para incluir essa informação no arquivo de relatório._

<br>

* Data do último registro de vendas.

![Evidência 11](/sprint_1/evidencias/evid_desafio/etapa4-1/11.jpg)                                     
_Evidência 11: Comando tail para mostrar as últimas informações do arquivo, -n 1 significa que quero mostrar apenas a última linha. Comando cut vai dividir as parte desta linha, utilizando o -d"," (delimitador vírgula) e mostrar o dado da posição -f5 (quinta coluna). Símbolos de maior (>>) para incluir essa informação no arquivo de relatório._

<br>

* A quantidade total de itens diferentes vendidos.

![Evidência 12](/sprint_1/evidencias/evid_desafio/etapa4-1/12.jpg)                                      
_Evidência 12: Comando cut vai dividir as parte das linhas deste arquivo, utilizando o -d"," (delimitador vírgula) e extrair os dados da posição -f2 (segunda coluna). Comando tail para mostrar as últimas informações do arquivo, -n +2 significa que quero mostrar apenas a partir da 2ª linha, nesta situação, ignorando o cabeçalho (produto). Comando uniq desconsidera informações duplicadas. Comando wc -l para contar a quantidade de dados. Símbolos de maior (>>) para incluir essa informação no arquivo de relatório._

<br>

* As 10 primeiras linhas do arquivo backup-dados-yyyymmdd.csv.

![Evidência 13](/sprint_1/evidencias/evid_desafio/etapa4-1/13.jpg)                                        
_Evidência 13: Comando head para mostrar as primeiras informações do arquivo, -n 11 significa que quero mostrar apenas as onze primeiras linhas, como quis manter o cabeçalho para visualização, não excluí a primeira linha. Símbolos de maior (>>) para incluir essa informação no arquivo de relatório._

<br>

* Resultado dessas tarefas

![Evidência 14](/sprint_1/evidencias/evid_desafio/etapa4-1/14.jpg)                                            
_Evidência 14: Resultado do primeiro relatório gerado._

<br>

* Comprimir o arquivo backup-dados-yyyymmdd.csv para o arquivo backup-dados-yyyymmdd.zip.

![Evidência 15](/sprint_1/evidencias/evid_desafio/etapa4-1/15.jpg)                                      
_Evidência 15: Comando zip para comprimir o arquivo._

<br>

* No diretório backup, apagar o arquivo backup-dados-yyyymmdd.csv. No diretório vendas, apagar o arquivo dados_de_vendas.csv.

![Evidência 16](/sprint_1/evidencias/evid_desafio/etapa4-1/16.jpg)                                    
_Evidência 16: Comando rm para apagar os arquivos .csv. Neste comando, como os arquivos estão em diretórios diferentes, colocar o endereço absoluto do arquivo, economiza linha de comando. Por experiência própria, prefiro utilizar endereço absoluto na maioria dos comandos._

<br>

### 4.2 Agendar a execução do processamento

* Criar um agendamento de execução de comandos Linux para executar o arquivo processamento_de_vendas.sh todos os dias de segunda a quinta-feira às 15h27.

![Evidência 17](/sprint_1/evidencias/evid_desafio/etapa4-2/17.jpg)                                                  
_Evidência 17: Comando crontab -e para gerar o agendamento de execução do script. Como foi a primeira vez utilizando este comando, foi solicitado para escolher qual editor de texto utilizar._

<br>

![Evidência 18](/sprint_1/evidencias/evid_desafio/etapa4-2/18.jpg)                                           
_Evidência 18: Comando de agendamento. Para os testes, coloquei para rodar o script de minuto a minuto e como tive dificuldades com a realização deles, acrescentei um arquivo teste.log para debugar. Por este mesmo motivo, não consegui que o agendamento acontecesse de segunda à quinta-feira, portanto defini que seria executado de quarta-feira à sábado._

<br>

* Conceder permissões para execução do script.

![Evidência 17](/sprint_1/evidencias/evid_desafio/etapa4-2/17.jpg)                                                
_Evidência 17: Comando chmod para alterar permissões do arquivo, +x para adicionar a permissão de execução._

<br>

### 4.3 Criar novo relatório

* Uma vez ao dia, modificar manual e completamente os dados do arquivo de vendas dados_de_vendas.csv que estão no diretório ecommerce.

![Evidência 19](/sprint_1/evidencias/evid_desafio/etapa4-3/19.jpg)                          
_Evidência 19: Solicitei para que o ChatGPT criasse para mim arquivos para alteração dos dados. Fui lapidando até que estivesse mais próximo do resultado esperado._                   
_Os arquivos estão disponíveis no diretório [evidencias](/sprint_1/evidencias/evid_desafio/arquivos_csv/)._

<br>

* Certificar-se que o arquivo processamento_de_vendas.sh esteja corretamente agendado.

![Evidência 20](/sprint_1/evidencias/evid_desafio/etapa4-3/20.jpg)                          
_Evidência 20: Comando sudo systemctl status cron para verificar se o agendamento está ativo. Os resultados que mostram as palavras destacadas em verde (enabled, active, running), informam que está agendado.                   

<br>

* Criar um script chamado consolidador_de_processamento_de_vendas.sh que une todos os relatórios gerados para um arquivo novo chamado relatorio_final.txt.

![Evidência 21](/sprint_1/evidencias/evid_desafio/etapa4-3/21.jpg)                          
_Evidência 21: Comando touch para criar um arquivo vazio, finalizando com extensão em .sh para determinar que é um arquivo executável. Comando nano para abrir o arquivo executável e codar.                    

<br>

![Evidência 22](/sprint_1/evidencias/evid_desafio/etapa4-3/22.jpg)                          
_Evidência 22: Comando cat para concatenar as informações dos relatórios. Símbolos de maior (>>) para incluir essas informações no arquivo de relatório final. Solicitei que acrescentasse ao relatório final uma linha em branco, através do comando echo, após cada acréscimo de informações dos relatórios, para que a visualização ficasse melhor.

<br>

* Após deversas execuções do script processamento_de_vendas.sh, executar manualmente o script consolidador_de_processamento_de_vendas.sh

![Evidência 23](/sprint_1/evidencias/evid_desafio/etapa4-3/23.jpg)                          
_Evidência 23: Comando ls para listar o conteúdo do diretório ecommerce. Comando bash para executar o script consolidador. Comando ls para informar que o relatorio final foi criado. Comando cat para mostrar no terminal o conteúdo do arquivo relatorio_final.txt, demonstrando assim, que a execução foi realizada com sucesso._

<br>

![Evidência 24](/sprint_1/evidencias/evid_desafio/etapa4-3/24.jpg)                          
_Evidência 24: Comando ls para listar o conteúdo do diretório backup, demonstrando que os arquivos csv foram comprimidos, e posteriormente apagados, e os relatórios gerados ao longo dos 4 dias. Comando ls para listar o conteúdo do diretório vendas, demonstrando que o arquivo .csv presente ali, também foi apagado._

<br>

