# :jigsaw: Desafio - Sprint 5

:calendar: 09/12/2024 à 23/12/2024

<br>

## :dart: Objetivo

Praticar os conhecimentos de AWS.

<br>

## :heavy_check_mark: Etapas

Obs.: Todas as evidências deste desafio encontram-se no diretório [evidências](../evidencias/evid_desafio/).

<br>

* [:scroll: 3. Preparação](#-3-preparação)
* [:package: 4. AWS S3 - Carregar arquivo para um bucket](#-4-aws-s3---carregar-arquivo-para-um-bucket)
* [:snake: 4. Python - Manipulação de Dados](#-4-python---manipulação-de-dados)
* [:dolphin: 5. Boto3 - Salvar resultado em .csv e enviar para o bucket](#-5-boto3---salvar-resultado-em-csv-e-enviar-para-o-bucket)

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

### :basket: 4. AWS S3 - Carregar arquivo para um bucket

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

<br><br>

### :snake: 4. Python - Manipulação de dados

<br>

* Criar outro script Python para criação de DataFrame com ``pandas`` ou ``polars`` para executar diversas manipulações.

![Evidência 04-0](../evidencias/evid_desafio/04-0.jpg)                       
_*Evidência 04-0 - Para melhor visualização e para escrever este relatório, fiz o script em ``.ipynb``, como exemplo. No arquivo entregue ``df_dados-abertos.py``, toda a importação dos dados segue, conforme solicitado, através do bucket.*_

<br>

![Evidência 04-1](../evidencias/evid_desafio/04-1.jpg)                       
_*Evidência 04-1 - Filtro utilizando dois operadores lógicos; Função de Data; Função de Conversão; .*_

<br>

![Evidência 04-2](../evidencias/evid_desafio/04-2.jpg)                       
_*Evidência 04-2 - Função de Agregação.*_

<br>

![Evidência 04-3](../evidencias/evid_desafio/04-3.jpg)                       
_*Evidência 04-3 - Filtro para obter o maior número de alunos por ingressante e para concluinte; Função de String;.*_

<br>

![Evidência 05](../evidencias/evid_desafio/05.jpg)                       
_*Evidência 05 - Resultado Final.*_


<br><br>

### :dolphin: 5. Boto3 - Salvar resultado em .csv e enviar para o bucket

* Após concluir as etapas anteriores, salvar o arquivo no formato ``.csv`` e enviar para o bucket, utilizando boto3.

![Evidência 06](../evidencias/evid_desafio/06.jpg)                       
_*Evidência 06 - Código realizado para subir arquivo ao bucket.*_

<br>

![Evidência 07](../evidencias/evid_desafio/07.jpg)                       
_*Evidência 07 - Resultado Final - Os dois arquivos solicitados já no bucket.*_

<br>

## :tada: Desafio concluído com sucesso! :champagne:
#### .... Feliz Natal e um 2025 cheio de luz e saúde ....                  
:santa: :christmas_tree: :mrs_claus: :ribbon: :snowman: :gift: :dove: :star2: :latin_cross: :pray: :candle: :bell: :love_letter: :fireworks:
<br><br>



<br><br>

:white_check_mark:
:sun_with_face:
