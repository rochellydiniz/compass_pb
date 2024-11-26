# :jigsaw: Desafio - Sprint 3
:calendar: 11/11/2024 à 25/11/2024

<br>

## :dart: Objetivo

Praticar Python combinando conhecimentos adquiridos no Programa de Bolsas.                         
Ler o arquivo de estatísticas da Loja do Google `googleplaystore.csv`, processar e gerar gráficos de análise.

<br>


## :thinking: Descrição 

Através de um dataset da loja de aplicativos da Google em .csv, criar um arquivo no formato .ipynb contendo código no modelo Notebook, com execução realizada. Incluir células em Markdown com documentação de cada célula de código criado

<br>

## :heavy_check_mark: Etapas

<br> 

* [:scroll: 3. Preparação](#3-preparação)
* [:open_file_folder: 4.1 Etapa 1: Ambiente](#-41-etapa-1-ambiente)
* [:woman_technologist:  4.2 Etapa 2: Desenvolvimento](#-42-etapa-2-desenvolvimento)


<br>

### :scroll: 3. Preparação

* Fazer o download do arquivo `googleplaystore.csv`, como arquivo base para manipulação, e construir ambiente utilizando Jupyter;


![Evidência 1](/sprint_3/evidencias/evid_desafio/ed_01.jpg)                           
_Evidência 1: Arquivo `googleplaystore.csv` aberto no VSCode, IDE que escolhi para criar o ambiente com Jupyter_    

<br><br>

### :open_file_folder: 4.1 Etapa 1: Ambiente

* Certificar-se que as bibliotecas `Pandas` e `Matplotlib` estão instaladas.                                  


![Evidência 2](/sprint_3/evidencias/evid_desafio/ed_02.jpg)                           
_Evidência 2: Como realizei os exercícios da sprint nesta mesma IDE, já tinha feito a instalação das bibliotecas. Portanto, bastou apenas a importação._

<br><br>

### :woman_technologist:  4.2 Etapa 2: Desenvolvimento

No desenvolvimento dessa sprint, foi solicitada a utilização de células em Markdown para documentação do código criado. Com isso, como no arquivo `.IPYINB` está tudo extremamente detalhado, neste `README.md`, resolvi apontar alguns detalhes e resultados que achei interessantes.

<br>

* Remoção das linhas duplicadas.

![Evidência 3](/sprint_3/evidencias/evid_desafio/ed_03.jpg)                           
_Evidência 3: Esta tabela é o resultado do código que verificou e removeu as duplicatas._

<br>

* Tratamento de dados: Foi necessário o tratamento e conversão de alguns dados por inconsistência e/ou tipos que impediam a manipulação adequada.

![Evidência 4](/sprint_3/evidencias/evid_desafio/ed_04.jpg)                           
_Evidência 4: A coluna `Installs` precisou ser tratada devido estar como tipo `string`, apesar de ser melhor classificada como tipo `integer`._

<br>

* O primeiro gráfico foi gerado pelo levantamento dos 05 aplicativos mais baixados. No entanto, como na coluna `Installs` não há valores precisos, aconteceram muitos empates. Para resolver quais aplicativos devem ficar no top 05, debatemos entre a turma e chegamos à conclusão que a coluna `Rating` seria um bom critério de desempate. Porém, pela avaliação de usuários possuir uma faixa de critério pequena com relação aos outros dados, também ocorrem muitos empates. Por fim, decidi que precisaria de um terceiro critério de desempate e defini a coluna `Reviews` para esta função.

![Evidência 5](/sprint_3/evidencias/evid_desafio/ed_05.jpg)                           
_Evidência 5: Gráfico `Top 5 Apps por Instalações`._

<br>

* Logo após foi solicitado o levantamento das categorias de apps existentes, gerando um gráfico de pizza. Eu gostaria de ter moldado melhor este gráfico, pois ficou feio esteticamente, além de confuso devido a sobreposição das legendas. Pra melhorar um pouco a visualização e sem interferir na mensagem principal, juntei todas as categorias com baixa representatividade (abaixo de 2%), em uma única chamada `OTHERS (Outros)`.

![Evidência 6](/sprint_3/evidencias/evid_desafio/ed_06.jpg)                           
_Evidência 6: Gráfico `Distribuição de Categorias`._

<br>

* Não só de gráficos se faz uma análise, então alguns levantamentos foram realizados sem a necessidade da criação dos mesmos.

![Evidência 7](/sprint_3/evidencias/evid_desafio/ed_07.jpg)                           
_Evidência 7: Qual o app mais caro existente no dataset?_

<br>

* A biblioteca Pandas permite a simplificação de muitas funções. Sem ela, o código abaixo teria muitas linhas a mais, comprometendo a velocidade de processamento da query.

![Evidência 8](/sprint_3/evidencias/evid_desafio/ed_08.jpg)                           
_Evidência 8: Quantos apps são classificados como para maiores de 17 anos?_

<br>

* Mais uma query atendida em poucas linhas.

![Evidência 9](/sprint_3/evidencias/evid_desafio/ed_09.jpg)                           
_Evidência 9: Top 10 apps por número de Reviews._

<br>

* Como última parte do desafio, foi solicitado que nós próprios criássemos mais dois cálculos sobre o dataset. Uma em lista:

![Evidência 10](/sprint_3/evidencias/evid_desafio/ed_10.jpg)                           
_Evidência 10: Top 15 aplicativos com mais tempo sem atualização_

<br>

* E outra em formato de valor:

![Evidência 11](/sprint_3/evidencias/evid_desafio/ed_11.jpg)                           
_Evidência 11: Qual o game mais caro do dataset?_

<br>


* Para concluir, foi necessário criar o gráfico das nossas análises, onde usamos a biblioteca `Matplotlib`. Obs.: não podendo gerar os mesmos tipos usados anteriormente.

![Evidência 12](/sprint_3/evidencias/evid_desafio/ed_12.jpg)                           
_Evidência 12: Gráfico `Top 15 aplicativos com mais tempo sem atualização`_

<br>


* Em Python as possibilidades são inumeráveis.

![Evidência 13](/sprint_3/evidencias/evid_desafio/ed_13.jpg)                           
_Evidência 13: Gráfico `Top 10 games mais caros`_

<br>

:white_check_mark:
:sun_with_face: