# :jigsaw: Desafio - Sprint 5

:calendar: 09/12/2024 à 23/12/2024

<br>

## :dart: Objetivo

Praticar os conhecimentos de AWS.

<br>

<!-- ## :thinking: Descrição 

Através de um dataset da loja de aplicativos da Google em .csv, criar um arquivo no formato .ipynb contendo código no modelo Notebook, com execução realizada. Incluir células em Markdown com documentação de cada célula de código criado

<br> -->

## :heavy_check_mark: Etapas

Obs.: Todas as evidências deste desafio encontram-se no diretório [evidências](../evidencias/evid_desafio/).

<br>

* [:scroll: 3. Preparação](#-3-preparação)
* [:package: 4.1 Etapa 1: Carguru](#-41-etapa-1-carguru)
* [:recycle:  4.2 Etapa 2: Reutilização de Containers](#️-42-etapa-2-reutilização-de-containers)
* [:performing_arts: 4.3 Etapa 3: Mascarar Dados](#-43-etapa-3-mascarar-dados)

<br>

### :scroll: 3. Preparação

 Procurar um arquivo .csv ou .json no [Portal de Dados Abertos](https://dados.gov.br/home) do Governo Federal. A princípio havia escolhido um dataframe que levantava os animais ameaçados de extinção no Brasil, porém, ao analisar friamente, percebi que não havia tipos de dados importantes para o que eu gostaria de apresentar. Então fiz outra busca e encontrei o atual:

<br>

> #### Dataset: Centro Federal de Educação Tecnológica de Minas Gerais - CEFET-MG
> Fonte: Dados Abertos - Governo Federal            
> Plano de Dados Abertos 2022-2024 / Última atualização: 14/02/2023         
> Relação de todos os alunos ingressantes e formandos, por ciclo acadêmico.        
> Disponível em ``https://dados.gov.br/dados/conjuntos-dados/12-alunos``

<br><br>

### :package: 4. AWS S3 - Carregar arquivo para um bucket

* Criação de um bucket novo para execução do desafio.
![Evidência 01](../evidencias/evid_desafio/01.jpg)                       
_*Evidência 01 - Página inicial dos buckets no serviço AWS S3. O bucket deste desafio é o ``desafio-dados.abertos``.*_

<br>

* A partir de um script Python, carregue o dataset para o bucket criado.

![Evidência 02-1](../evidencias/evid_desafio/02-1.jpg)    
![Evidência 02-2](../evidencias/evid_desafio/02-2.jpg)                    
_*Evidência 02 - Código em Python para baixar o dataset diretamente da fonte e subir para o bucket. Importante falar aqui sobre a função ``load_dotenv``. Esta função é utilizada para carregar variáveis de ambiente de um arquivo ``.env`` para o ambiente do sistema. Sendo útil para manter informações sensíveis, como chaves de API e senhas, fora do código-fonte, ao utilisar ``load_dotenv``, essas variáveis podem ser acessadas no código como se fossem definidas diretamente no ambiente do sistema. Neste caso, para manter os dados em sigilo, ao subir no Git, fiz um ``gitignore`` para o arquivo ``.env``.*_

![Evidência 02-3](../evidencias/evid_desafio/02-3.jpg)


<br>

* O script Pyhton enviou o arquivo .csv para o bucket com sucesso.

![Evidência 03](../evidencias/evid_desafio/03.jpg)                       
_*Evidência 03 - Arquivo ``PDA_2022-2024_1.2_Alunos_Anonimo.csv`` no bucket.*_

<br>

* Criar outro script Python para criação de DataFrame com ``pandas`` ou ``polars`` para executar diversas manipulações.

![Evidência 04-0](../evidencias/evid_desafio/04-0.jpg)                       
_*Evidência 04-0 - Para melhor visualização e para escrever este relatório, fiz o script em ``.ipynb``, como exemplo. No arquivo entregue ``df_dados-abertos.py``, toda a importação dos dados segue, conforme solicitado, através do bucket.*_

<br>

![Evidência 04-1](../evidencias/evid_desafio/04-1.jpg)                       
_*Evidência 04-1 - Filtro utilizando dois operadores lógicos; Função de Data; Função de Conversão; .*_


<br><br>

### :recycle: 4.2 Etapa 2: Reutilização de Containers

<br>

Nesta etapa foi realizada a seguinte pergunta a ser respondida num arquivo Markdown:

<br>

> ***É possível reutilizar containers? Em caso positivo, apresente o comando necessário para reiniciar um dos containers parados em seu ambiente Docker. Não sendo possível reutilizar, justifique sua resposta.***

<br>

_*Sim, é possível reutilizar containers com o comando ``restart``.             
Usando o caso da etapa 1, ficaria da seguinte forma:
``docker restart 6ffbfaea0b18`` ou ``docker restart adoring_lehmann``.*_

<br>

![Evidência Et02-01](../evidencias/evid_desafio/etapa-02/01.jpg)               
_*Evidência et02-01 - Executei o comando `restart` utilizando a etapa-01 como exemplo. No primeiro momento na listagem dos containers, é possível verificar que o container havia passado pelo restart há 4 horas, no meu primeiro teste. Logo após, "restartei" novamente, trazendo numa posterior consulta, a informação que o mesmo container foi reinicializado há 07 segundos.*_


<br><br>

### :performing_arts: 4.3 Etapa 3: Mascarar Dados

Na última etapa, foi solicitada a criação de um container que pudesse receber inputs durante a execução. Para isso, precisávamos criar um script Python que implementasse o mascaramento das palavras recebidas através do hash SHA-1.

![Evidência Et03-01](../evidencias/evid_desafio/etapa-03/01.jpg)                          
_*Evidência et03-01 - Script Pyhton criado com as instruções solicitadas.*_

<br>

![Evidência Et03-02](../evidencias/evid_desafio/etapa-03/02.jpg)                                      
_*Evidência et03-02 - Arquivo Dockerfile para criação da imagem.*_

<br>

![Evidência Et03-03](../evidencias/evid_desafio/etapa-03/03.jpg)                                      
_*Evidência et03-03 - Dentre as instruções, foi solicitado que a tag dessa imagem fosse ``mascarar-dados``.*_

<br>

![Evidência Et03-04](../evidencias/evid_desafio/etapa-03/04.jpg)                                      
_*Evidência et03-04 - Imagem criada conforme solicitado.*_

<br>

![Evidência Et03-05](../evidencias/evid_desafio/etapa-03/05.jpg)                                      
_*Evidência et03-05 - Container rodando e os resultados da execução do script.*_



<br><br>

:white_check_mark:
:sun_with_face:
