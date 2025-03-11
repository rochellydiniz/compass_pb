-- dim_titulo
CREATE VIEW desafiofinal_refined_data.dim_titulo AS
SELECT 
    id_titulo,
    tp_titulo,
    nm_titulo_principal,
    nm_titulo_original,
    sg_pais_origem,
    sg_idioma
FROM desafiofinal_trusted_data.tb_titulo;

------------------------------
-- dim_genero
CREATE VIEW desafiofinal_refined_data.dim_genero AS
SELECT 
    id_genero,
    nm_genero
FROM desafiofinal_trusted_data.tb_genero;

------------------------------
-- dim_keyword
CREATE VIEW desafiofinal_refined_data.dim_keyword AS
    SELECT 
    id_keyword,
    ds_keyword
FROM desafiofinal_trusted_data.tb_keyword;

------------------------------
-- dim_classificacao

    CREATE VIEW desafiofinal_refined_data.dim_classificacao AS
    SELECT 
        sg_pais_class_indicativa,
        cd_class_indicativa,
        nr_idade_classificacao
    FROM desafiofinal_trusted_data.tb_class_indicativa

------------------------------
-- dim_titulo_genero

    CREATE VIEW desafiofinal_refined_data.dim_titulo_genero AS
    SELECT 
        id_titulo,
        tp_titulo,
        id_genero
    FROM desafiofinal_trusted_data.tb_assoc_genero_titulo

------------------------------
-- dim_titulo_keyword

    CREATE VIEW desafiofinal_refined_data.dim_titulo_keyword AS
    SELECT 
        id_titulo,
        tp_titulo,
        id_keyword
    FROM desafiofinal_trusted_data.tb_assoc_keyword_titulo

------------------------------
-- dim_titulo_classificacao

    CREATE VIEW desafiofinal_refined_data.dim_titulo_classificacao AS
    SELECT 
        id_titulo,
        tp_titulo,
        sg_pais_class_indicativa
    FROM desafiofinal_trusted_data.tb_assoc_class_indicativa_titulo