# :jigsaw: Desafio - Sprint 2

<br>

## :dart: Objetivo

Praticar conhecimentos de Modelagem de Dados Relacional e Dimensional com Linguagem SQL.

<br>

## :thinking: Descrição 

Através dos dados de locação de veículos de uma concessionária, normalizar a base de dados e converter o modelo relacional em modelo dimencional.

<br>

## :heavy_check_mark: Etapas


* [:scroll: 3. Preparação](#3-preparação)
* [:woman_technologist: 4.1. Normalizar Base de Dados](#-41-normalizar-base-de-dados)
* [:chart_with_upwards_trend: 4.2. Modelo Dimensional baseado no Modelo Relacional](#42-modelo-dimensional-baseado-no-modelo-relacional)

<br>

### :scroll: 3. Preparação

* Fazer o download do arquivo concessionaria.zip.

![Evidência 1](/sprint_2/evidencias/evid_desafio/01.jpg)                           
_Evidência 1: Arquivo concessionaria.zip na seção "Downloads" do navegador_              
<br>

* Assistir ao Vídeo Explicativo             

![Evidência 2](/sprint_2/evidencias/evid_desafio/02.jpg)                                   
_Evidência 2: Vídeo: Explicação Desafio._                        
<br>

* Após descompactar, abrir o arquivo .sqlite na ferramenta DBeaver.

![Evidência 3](/sprint_2/evidencias/evid_desafio/03.jpg) 
![Evidência 4](/sprint_2/evidencias/evid_desafio/04.jpg)                                      
_Evidências 3 e 4: Abrir base de dados no DBeaver._

<br>


### :woman_technologist: 4.1 Normalizar Base de Dados

* A base de dados fornecida possui apenas uma tabela com os dados de locação que necessitam ser normalizadas.

![Evidência 5](/sprint_2/evidencias/evid_desafio/05.jpg)                                    
_Evidência 5: Tabela Locação fornecida._                               

<br>

* Primeiro fiz a seleção de tudo o que continha na tabela para visualização dos dados e, de acordo com a **NF1** (normal form / FN - forma normal), os valores na tabela são únicos, podendo seguir para as próximas etapas.

![Evidência 6](/sprint_2/evidencias/evid_desafio/06.jpg)
_Evidência 6: Comando ***SELECT***  * para selecionar todos os dados._

<br>

* Quanto aos critérios NF2 e NF3, a tabela precisa de normalizações. Aplicando a **NF2**, criei novas tabelas para separar as dependências parciais.

![Evidência 7](/sprint_2/evidencias/evid_desafio/07.jpg)                      
_Evidência 7: Comando ***CREATE TABLE***  * para criar nova tabela._

<br>

* Foram criadas as tabelas tb_carro, tb_cliente, tb_combustivel e tb_vendedor.            

![Evidência 8](/sprint_2/evidencias/evid_desafio/08.jpg)  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ![Evidência 9](/sprint_2/evidencias/evid_desafio/09.jpg)                      
_Evidências 8 e 9: Lista de tabelas criadas. + Tabela tb_vendedor vazia._

<br>

* Após a criação, fiz a inserção dos dados da tb_locação para as tabelas corretas.  

![Evidência 10](/sprint_2/evidencias/evid_desafio/10.jpg)                      
_Evidência 10: Comando ***INSERT INTO*** para inserir os dados dos campos discriminados da tb_locacao. Utilizado ***SELECT DISTINCT*** para não se repetirem os dados._

<br>

* A cada novo bloco inserção, fiz o SELECT para verificar se o resultado estava de acordo com o esperado.

![Evidência 11](/sprint_2/evidencias/evid_desafio/11.jpg)                      
_Evidência 11: tb_cliente preenchida conforme o esperado. NF2 concluída._

<br>

* Ao analisar os dados, verifiquei que para a ***NF3***, nesta base, seria necessário criar uma tabela para combustível, mas que estivesse relacionada com tb_carro, e não com tb_locacao, já que essa é característica do veículo. Como fiz essa conclusão antes mesmo de iniciar a NF2, resolvi já incluir uma chave estrangeira na criação da tb_carro, relacionando com a tb_combustivel.

![Evidência 12](/sprint_2/evidencias/evid_desafio/12.jpg)                      
_Evidência 12: Após declarar as colunas na criação da tabela, utilizei o comando *FOREIGN KEY* para fazer o link da coluna idCombustível entre tb_carro e tb_combustivel. NF3 concluída._

<br>

* Neste caso, para não perder nenhuma possível informação importante, renomeei a tabela locação para tb_locacao_original.

![Evidência 13](/sprint_2/evidencias/evid_desafio/13.jpg) &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ![Evidência 14](/sprint_2/evidencias/evid_desafio/14.jpg)                      
_Evidências 13 e 14: Comando ***ALTER TABLE + RENAME TO*** para altarar o nome da tabela._

<br>

* Criei uma nova tb_locacao para reunir todos os dados necessários próprios e das outras tabelas através das chaves estrangeiras. Logo após, inseri os dados.

![Evidência 15](/sprint_2/evidencias/evid_desafio/15.jpg)    &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;    ![Evidência 16](/sprint_2/evidencias/evid_desafio/16.jpg)                      
_Evidências 15 e 16: Criação da nova tabela Locação. + Inserção dos dados vindos da tb_locacao_original._

<br>

* Visualização de como ficou a tb_locacao aplicadas as normalizações.

![Evidência 17](/sprint_2/evidencias/evid_desafio/17.jpg)               
_Evidência 17: Resultado do comando ***SELECT*** para a tabela normalizada. _

<br>

* Os valores das colunas dataLocacao e dataEntrega, por algum motivo do DBeaver, não manteve a visualização na formatação correta. Mas foi verificado que os dados foram inseridos corretamente e com tipo correto (DATE).

![Evidência 18](/sprint_2/evidencias/evid_desafio/18.jpg)               
_Evidência 18: Resultado da consulta ***SELECT * FROM tb_locacao*** em forma de texto puro formatado, que comprova que as datas estão em formato YYYYMMDD._

<br>

* Na criação do Modelo Relacional, se fosse para manter desta forma, a tb_locacao_original continuaria presente.

![Evidência 19](/sprint_2/evidencias/evid_desafio/19.png)               
_Evidência 19: Modelo Relacional mostrando a presença da tb_locacao_original sem nenhum relacionamento com outra tabela._

<br>

* Portanto, como todos os dados foram utilizados na nova tb_locacao e as outras tabelas criadas, não fazia mais sentido manter a original, podendo ser excluída.

![Evidência 20](/sprint_2/evidencias/evid_desafio/20.jpg)  
![Evidência 21](/sprint_2/evidencias/evid_desafio/21.jpg)               
_Evidências 20 e 21: Comando ***DROP TABLE*** para excluir a tabela da base de dados. + Quando esse comando é executado, o DBeaver abre uma janela pop-up para alertar sobre a possível perda de dados._

<br> <br>


### :chart_with_upwards_trend: 4.2 Modelo Dimensional baseado no Modelo Relacional

<br>

* Após concluir toda a etapa anterior, foi necessário apresentar o Modelo Relacional após normalização da base de dados.

![Evidência 22](/sprint_2/evidencias/evid_desafio/22.png)               
_Evidência 22: ***Modelo Relacional*** extraído do DBeaver após normalização._

<br>

* A partir do Modelo Relacional, foi feito o Modelo Dimensional baseado na evidência anterior. Foi utilizado o modelo ***Star Schema*** para otimizar em tempo de processamento as consultas em grandes volume de dados, sendo as colunas da tb_combustível (no modelo relacional) incorporadas à tabela dim_carro. Aqui, a tabela fato fica no centro da estrela (fato_locacao), interligando-se com as tabelas dimensões como pontas da estrela (dim carro, dim_cliente e dim_vendedor) através de chaves estrangeiras. Suas relações 1 pra N e 1 pra 1, significam que uma locação só pode estar vinculada para um cliente, um carro e um vendedor (1:1). Já no caminho inverso, um carro, um cliente e um vendedor podem fazer e estar presentes em várias locações (1:n).

![Evidência 23](/sprint_2/evidencias/evid_desafio/23.png)               
_Evidência 23: ***Modelo Dimensional*** com tabela fato e tabelas dimensionais._

<br>

As imagens dos modelos estão no diretório [desafio](/sprint_2/desafio/).

<br>

:white_check_mark:
:sun_with_face: