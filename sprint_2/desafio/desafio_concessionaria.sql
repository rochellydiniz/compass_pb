---- A tabela locação atende ao critério NF1?
------ Sim, pois os valores são únicos em todos os campos.
SELECT *
FROM tb_locacao tl;


---- A tabela locação atende ao critério NF2 e NF3?
------ Não, é preciso criar novas tabelas para separar as dependências parciais (NF2).
-------- Criação das tabelas tb_cliente, tb_carro, tb_combustivel, tb_vendedor, e seus respectivos dados.
-------- Para a tabela carro, será preciso relacionar com a tabela combustível através de uma chave estrangeira (NF3). 

-- Cliente
DROP TABLE IF EXISTS tb_cliente; 

CREATE TABLE tb_cliente (
	idCliente INTEGER PRIMARY KEY NOT NULL
,	nomeCliente	VARCHAR NOT NULL
,	cidadeCliente VARCHAR NOT NULL
,	estadoCliente VARCHAR NOT NULL
,	paisCliente VARCHAR NOT NULL
);




-- Combustível
DROP TABLE IF EXISTS tb_combustivel; 

CREATE TABLE tb_combustivel (
	idCombustivel INTEGER PRIMARY KEY NOT NULL
,	tipoCombustivel	VARCHAR NOT NULL
);

-- Carro

CREATE TABLE tb_carro (
	idCarro INTEGER PRIMARY KEY NOT NULL
,	classiCarro VARCHAR NOT NULL
,	marcaCarro VARCHAR NOT NULL
,	anoCarro INTEGER NOT NULL
,	idCombustivel INTEGER NOT NULL
,	FOREIGN KEY (idCombustivel) REFERENCES tb_combustivel (idCombustivel)
);

-- Vendedor

CREATE TABLE tb_vendedor (
	idVendedor INTEGER PRIMARY KEY NOT NULL
,	nomeVendedor VARCHAR NOT NULL
,	sexoVendedor SMALLINT NOT NULL
,	estadoVendedor VARCHAR NOT NULL
);

------- Inserção dos dados nas novas tabelas
-- Cliente
INSERT INTO tb_cliente
SELECT DISTINCT 
	idCliente 
,	nomeCliente
,	cidadeCliente 
,	estadoCliente 
,	paisCliente 
FROM tb_locacao
;

SELECT * FROM tb_cliente tc ;

-- Combustível
INSERT INTO tb_combustivel 
SELECT DISTINCT 
	idCombustivel
,	tipoCombustivel
FROM tb_locacao
;

SELECT * FROM tb_combustivel tc ;

-- Carro
INSERT INTO tb_carro 
SELECT DISTINCT 
	idCarro
,	classiCarro
,	marcaCarro
,	anoCarro
,	idCombustivel 
FROM tb_locacao
;

SELECT * FROM tb_carro tc ;

--Vendedor
INSERT INTO tb_vendedor 
SELECT DISTINCT 
	idVendedor
,	nomeVendedor
,	sexoVendedor
,	estadoVendedor
FROM tb_locacao
;

SELECT * FROM tb_vendedor tv ;

--- Nesta situação, vou renomear a tabela locação original, para não perde-la

ALTER TABLE tb_locacao RENAME TO tb_locacao_original ;

--- Agora, criarei uma nova tabela locação, para reunir todos os dados necessários próprios e das outras tabelas.
--- Logo após será feita a inserção dos dados.
DROP TABLE IF EXISTS tb_locacao;

CREATE TABLE tb_locacao (
	idLocacao INTEGER PRIMARY KEY NOT NULL
,	idCliente INTEGER NOT NULL
,	idCarro INTEGER NOT NULL
,	kmCarro INTEGER NOT NULL
,	dataLocacao DATE NOT NULL
,	horaLocacao TIME NOT NULL
,	qtdDiaria INTEGER NOT NULL
,	vlrDiaria DECIMAL NOT NULL
,	dataEntrega DATE NOT NULL
,	horaEntrega TIME NOT NULL
,	idVendedor INTEGER NOT NULL
,	FOREIGN KEY (idCliente) REFERENCES tb_cliente (idCliente)
,	FOREIGN KEY (idCarro) REFERENCES tb_carro (idCarro)
,	FOREIGN KEY (idVendedor) REFERENCES tb_vendedor (idVendedor)
);

INSERT INTO tb_locacao 
SELECT DISTINCT 
	idLocacao
,	idCliente
,	idCarro
,	kmCarro
,	dataLocacao 
,	horaLocacao
,	qtdDiaria
,	vlrDiaria
,	dataEntrega
,	horaEntrega
, 	idVendedor 
FROM tb_locacao_original
;

SELECT * FROM tb_locacao ;


--- Inconsistências com as datas das colunas 'dataLocacao' e 'dataEntrega'

--------- No DBeaver, na visualização Texto, é possível verificar que as datas estão no padrão correto, então não há alteração no código a ser realizado.
--------- Mas, para não ficar confuso com relação à formatação, no modo Grade, apertei o botão direito do mouse em cima do nome coluna, 
--------- e selecionei Visualizar/Formatar > Formato de exibição do valor > Nativo do banco de dados


---- Como todos os dados e colunas foram utilizados na tb_locacao, já é possível excluir tb_locacao_original 

DROP TABLE tb_locacao_original; 


