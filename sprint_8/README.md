# :hourglass_flowing_sand: Sprint 8 - AWS e Spark
:calendar: 20/01 à 03/02/2025


### :writing_hand: Resumo

- 


<br>

### :trophy: Certificados

Nessa Sprint não houve curso externo, portanto, não há certificado a ser apresentado.

<br>

### :jigsaw: Desafio

 O objetivo dessa sprint é complementa os dados dos Filmes e Séries com dados oriundos da API do TMDB.
 O relatório do desafio e os arquivos gerados estão no diretório [desafio](./desafio/README.md).

<br>

### :brain: Exercícios

<br>

Todos os prints gerados estão disponíveis no diretório [exercicios](./exercicios/) e os seus arquivos de resultados se encontram em [evidencias](./evidencias/evid_exercicios/).

Abaixo relaciono alguns que gostaria de compartilhar.

<br>

#### 7 - Lab Glue

Construção de um processo de ETL simplificado utilizando AWS Glue.

<br>

* Foi fornecido um arquivo ``.csv`` para ser salvo em um bucket no S3 com path determinado.                         

![Evidência Lab7-1](./evidencias/evid_exercicios/7_Lab_AWS_Glue/1.jpg)              
_*Evidência Lab7-1 - Arquivo salvo conforme determinado.*_

<br>

* Também foi ensinada a criação de Role (através do IAM) para os Jobs do Glue.                         

![Evidência Lab7-2](./evidencias/evid_exercicios/7_Lab_AWS_Glue/2.jpg)              
_*Evidência Lab7-2 - Criado Role ``AWSGlueServiceRole-Lab4`` associada às políticas geridas pela AWS.*_

<br>

* Após muito outros passos de configuração, criamos o Job.                         

![Evidência Lab7-3](./evidencias/evid_exercicios/7_Lab_AWS_Glue/3.jpg)              
_*Evidência Lab7-3 - Assim como nos exercícios anteriores, primeiro criamos um teste do conteúdo da Udemy e depois fazemos nosso próprio exercício.*_

<br>

* Código feito, após alguns erros no caminho, o job é executado com sucesso.                         

![Evidência Lab7-4](./evidencias/evid_exercicios/7_Lab_AWS_Glue/4.jpg)              
_*Evidência Lab7-4 - Resultado do script codado no Athena. O código deve ler o arquivo do bucket, imprimir o schema, deixar os nomes em ``CAIXA ALTA``, imprimir a contagem de linhas, de nomes, dentre outras especificações.*_

<br>

* Alguns requisitos foram dados para o arquivos de saída.                         

![Evidência Lab7-5](./evidencias/evid_exercicios/7_Lab_AWS_Glue/5.jpg)              
_*Evidência Lab7-5 - A gravação deve ser em path específico, em formato JSON e particionados por ``Sexo`` e ``Ano``.*_

<br>

* Resultando nos diretórios apresentados.                         

![Evidência Lab7-6](./evidencias/evid_exercicios/7_Lab_AWS_Glue/6.jpg)              
_*Evidência Lab7-6 - Foram divididos primeiramente em sexo e depois em anos.*_

<br>

* Foi dada a dica de verificar os Logs de execução.                         

![Evidência Lab7-7](./evidencias/evid_exercicios/7_Lab_AWS_Glue/7.jpg)              
_*Evidência Lab7-7 - Logs de Execução.*_

<br>

* E mais detalhadamente no ``CloudWatch continuous logs``.                         

![Evidência Lab7-8](./evidencias/evid_exercicios/7_Lab_AWS_Glue/8.jpg)              
_*Evidência Lab7-8 - Detalhes do grupo de logs.*_

<br>

* Por fim, foi criado um ``Crawler``, que cria uma tabela a partir do s dados escritos no S3 de forma automática.                         

![Evidência Lab7-9](./evidencias/evid_exercicios/7_Lab_AWS_Glue/9.jpg)              
_*Evidência Lab7-9 - Neste exercício foi determinado que a frequência de execução fosse realizada apenas ``on demand``.*_

#### 5 - Exercício TMDB (Já realizado na Sprint 7)

Desenvolvimento de job de processamento com o framework Spark por meio de container Docker.


* Para começar, foi necessário o ``pull`` de uma imagem para utilizar o Jupyter pelo Shell do Spark.                          

![Evidência Ex5-1](./evidencias/evid_exercicios/5_Ex_Apache_Spark/1.jpg)              
_*Evidência Ex5-1 - Container rodando, utilizando comando complementado de ``-it`` para execução de forma interativa.*_

<br>


* O container fornece um link para acessar o Jupyter Lab no navegador.              

![Evidência Ex5-2](./evidencias/evid_exercicios/5_Ex_Apache_Spark/2.jpg)              
_*Evidência EX5-2 - Interface do Jupyter Lab.*_

<br>

* Criado código para um contador de palavras de um arquivo Markdown. Depois de alguns testes, cheguei ao código final.          

![Evidência Ex5-3](./evidencias/evid_exercicios/5_Ex_Apache_Spark/3.jpg)              
_*Evidência Ex5-3 - Não consegui baixar o arquivo ``.py`` que foi gerado no shell, então a evidência que tenho é esse print.*_

<br>

* Utilizei um README de sprints anteriores para usar no exercício. Refinei da forma que achei mais prudente, desconsiderando "palavras" com menos de 4 letras, algumas preposições, artigos, caracteres especiais etc.                         

![Evidência Ex5-4](./evidencias/evid_exercicios/5_Ex_Apache_Spark/4.jpg)              
_*Evidência Ex5-4 - Ao final, o resultado foi impresso no terminal.*_

<br>

* Este foi o resultado do script, após refinamento.                             

![Evidência Ex5-5](./evidencias/evid_exercicios/5_Ex_Apache_Spark/5.jpg)              
_*Evidência Ex5-5 - Sendo ``evidência`` palavra mais utilizada.*_

<br><br>

#### 6 - Exercício TMDB

Criação de processo de extração de dados da API do TMDB utilizando serviços AWS. O resultado do exercício está na pasta de [exercícios](./exercicios/)


<br>

* O primeiro passo deste exercício é a criação de conta no TMDB.                         

![Evidência Ex6-1](./evidencias/evid_exercicios/6_Ex_TMDB/1.jpg)              
_*Evidência Ex6-1 - Após algumas instruções de configuração o TMDB fornece a chave token que permite as buscas da API.*_

<br>

* No conteúdo presente na Udemy, nos é apresentado um passo-a-passo para criar um teste utilizando endpoint de filme e, após isso, executarmos o exercício.                         

* Para o meu código, utilizei o endpoint de séries para mostrar os títulos melhores avaliados. Apesar de parecer ser igual ao de séries, é possível analisar no código que algumas colunas não são iguais nos dois tipos.

![Evidência Ex6-2](./evidencias/evid_exercicios/6_Ex_TMDB/2.jpg)              
_*Evidência Ex6-2 - Resultado do script que ao final é salvo também em arquivo ``.csv``.*_


<br><br>

