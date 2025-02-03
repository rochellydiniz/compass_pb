# :hourglass_flowing_sand: Sprint 8 - AWS, Spark e Camada Trusted
:calendar: 20/01 à 03/02/2025


### :writing_hand: Resumo

* Esta sprint foi elaborada para realização do desafio, trabalhando com a camada Trusted e processamento de dados em nuvem.

<br>

### :trophy: Certificados

Nessa Sprint não houve curso externo, portanto, não há certificado a ser apresentado.

<br>

### :jigsaw: Desafio

 O objetivo dessa sprint é trabalhar a camada Trusted, transformando o os arquivos da camada Raw em arquivos Parquet para padronização.             
 O relatório do desafio e os arquivos gerados estão no diretório [desafio](./desafio/README.md).

<br>

### :brain: Exercícios

<br>

Todos os prints gerados estão disponíveis no diretório [exercicios](./exercicios/) e os seus arquivos de resultados se encontram em [evidencias](./evidencias/evid_exercicios/).

Abaixo relaciono alguns que gostaria de compartilhar.

<br>

#### 2.2 - Exercício Spark Batch - Geração e massa de dados

Gerar dados para serem processados, via Apache Spark.

<br>

##### Etapa 1

* Criar uma lista de 250 números inteiros aleatórios, reverter a lista e imprimi-la.                         

![Evidência 1](./evidencias/evid_exercicios/2.2_ex_spark_batch/1.jpg)              
_*Evidência Ex2.2-1 - Resultado da etapa 1.*_

<br>

##### Etapa 2

* Trabalhar com uma lista de nomes de 20 animais, ordená-los, salvar em um arquivo CSV e imprimi-los.
                         

![Evidência 2](./evidencias/evid_exercicios/2.2_ex_spark_batch/2.jpg)              
_*Evidência Ex 2.2-2 - Resultados em código da etapa 2.*_

<br>

![Evidência 3](./evidencias/evid_exercicios/2.2_ex_spark_batch/3.jpg)              
_*Evidência Ex 2.2-2 - Resultados da etapa 2 no csv gerado.*_

<br>

##### Etapa 3

* Gerar um dataset de nomes de pessoas. Deixei como [evidência](./evidencias/evid_exercicios/2.2_ex_spark_batch/3.3_etapa_3.ipynb) o notebook mostrando os resultados de cada passo.

![Evidência 4](./evidencias/evid_exercicios/2.2_ex_spark_batch/4.jpg)              
_*Evidência Ex 2.2-3 - Resultados da etapa 3 em código.*_

<br>

![Evidência 5](./evidencias/evid_exercicios/2.2_ex_spark_batch/5.jpg)              
_*Evidência Ex 2.2-3 - O ``.txt`` foi gerado, e o conteúdo foi bem grande, tanto que tive dificuldades para abri-lo.*_

<br><br>

#### 2.2 - Exercício Spark Batch - Geração e massa de dados

* Aplicar recursos básicos de manipulação de DataFrames via Spark.

<br>

* Etapa 1 - Configurar a sessão Spark e carregar os dados

![Evidência 1](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/1.jpg)              
_*Evidência Ex 2.3-1 - Resultado etapa 1.*_

<br>

* Etapa 2 - Renomear colunas e explorar Schema

![Evidência 2](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/2.jpg)              
_*Evidência Ex 2.3-2 - Resultado etapa 2.*_

<br>

* Etapa 3 - Adicionar coluna escolaridade

![Evidência 3](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/3.jpg)              
_*Evidência Ex 2.3-3 - Resultado etapa 3.*_

<br>

* Etapa 4 - Adicionar coluna país

![Evidência 4](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/4.jpg)              
_*Evidência Ex 2.3-4 - Resultado etapa 4.*_

<br>

* Etapa 5 - Adicionar coluna AnoNascimento (valores aleatórios entre 1945 e 2010)

![Evidência 5](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/5.jpg)              
_*Evidência Ex 2.3-5 - Resultado etapa 5.*_

<br>

* Etapa 6 - Filtrar pessoas nascidas nesse século

![Evidência 6](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/6.jpg)              
_*Evidência Ex 2.3-6 - Resultado etapa 6.*_

<br>

* Etapa 7 - Filtrar pessoas nascidas nesse século usando Spark SQL

![Evidência 7](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/7.jpg)              
_*Evidência Ex 2.3-7 - Resultado etapa 7.*_

<br>

* Etapa 8 - Contar Millennials (1980-1994)

![Evidência 8](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/8.jpg)              
_*Evidência Ex 2.3-8 - Resultado etapa 8.*_

<br>

* Etapa 9 - Contar Millennials (1980-1994) usando Spark SQL

![Evidência 9](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/9.jpg)              
_*Evidência Ex 2.3-9 - Resultado etapa 9.*_

<br>

* Etapa 10 - Contar gerações por país

![Evidência 10](./evidencias/evid_exercicios/2.3_ex_spark_batch_nomes/10.jpg)              
_*Evidência Ex 2.3-10 - Resultado etapa 10.*_

<br><br>

#### 5 - Exercício TMDB

Criação de processo de extração de dados da API do TMDB utilizando serviços AWS. O resultado do exercício está na pasta de [exercícios](./exercicios/). 

*_O conteúdo deste exercício foi copiado da Sprint 7, pois são iguais_*


<br>

* O primeiro passo deste exercício é a criação de conta no TMDB.                         

![Evidência Ex6-1](./evidencias/evid_exercicios/5_Ex_TMDB/1.jpg)              
_*Evidência Ex6-1 - Após algumas instruções de configuração o TMDB fornece a chave token que permite as buscas da API.*_

<br>

* No conteúdo presente na Udemy, nos é apresentado um passo-a-passo para criar um teste utilizando endpoint de filme e, após isso, executarmos o exercício.                         

* Para o meu código, utilizei o endpoint de séries para mostrar os títulos melhores avaliados. Apesar de parecer ser igual ao de séries, é possível analisar no código que algumas colunas não são iguais nos dois tipos.

![Evidência Ex6-2](./evidencias/evid_exercicios/5_Ex_TMDB/2.jpg)              
_*Evidência Ex6-2 - Resultado do script que ao final é salvo também em arquivo ``.csv``.*_


<br><br>

