-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Asset Depreciation report

-- COMMAND ----------

list 'abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0'

-- COMMAND ----------

USE prod_gbs_finance_latam.silver_assets;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Tabela de classe do imobilizado

-- COMMAND ----------

--CREATE A VIEW ON TOP OF ALL THE FILES TARGET TO INGEST

CREATE OR REPLACE TEMP VIEW ax_deprec_classe_vw
USING CSV
OPTIONS (
  path = "abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0/Classe Imobilizado.csv",  --It is possible to use WILDCARD character "*"
  header = "true",
  delimiter = ";",
  inferSchema = "true",
  mode = "FAILFAST"  -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
);


select * from ax_deprec_classe_vw;

-- COMMAND ----------

use prod_gbs_finance_latam.silver_assets;
create or replace table ax_deprec_classe as

select
  distinct Classe as CLASSE,
  `Denom.classe imob.` as CLASSE_DENOMINACAO,
  `Descrição da classe do imobilizado` as CLASSE_DESCRICAO,
  `L/P dprNor` as CONTA,
  `Descrição Conta L/P dprNor` as CONTA_DRECICAO
  
from
  ax_deprec_classe_vw;

-- COMMAND ----------

-- MAGIC %md ##Tabela de contas contabeis

-- COMMAND ----------

--CREATE A VIEW ON TOP OF ALL THE FILES TARGET TO INGEST

CREATE OR REPLACE TEMP VIEW ax_deprec_contas_vw
USING CSV
OPTIONS (
  path = "abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0/Contas Contábeis.csv",  --It is possible to use WILDCARD character "*"
  header = "true",
  delimiter = ";",
  inferSchema = "true",
  mode = "FAILFAST"  -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
);


select * from ax_deprec_contas_vw;

-- COMMAND ----------

use prod_gbs_finance_latam.silver_assets;
create or replace table ax_deprec_contas as

select
  Empresa as COCD,
  Conta as CONTA,
  Grupo as HFM_GROUP,
  `Descrição` as DESCRIPTION
  
from
  ax_deprec_contas_vw;

-- COMMAND ----------

-- MAGIC %md ##Tabela de centro de custos

-- COMMAND ----------

--CREATE A VIEW ON TOP OF ALL THE FILES TARGET TO INGEST

CREATE OR REPLACE TEMP VIEW ax_deprec_cc_vw
USING CSV
OPTIONS (
  path = "abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0/Centros de Custos.csv",  --It is possible to use WILDCARD character "*"
  header = "true",
  delimiter = ";",
  inferSchema = "true",
  mode = "FAILFAST"  -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
);


select * from ax_deprec_cc_vw;

-- COMMAND ----------

use prod_gbs_finance_latam.silver_assets;
create or replace table ax_deprec_cc as

select
  Empresa as COCD,
  Conta as CONTA,
  Grupo as HFM_GROUP,
  `Descrição` as DESCRIPTION
  
from
  ax_deprec_contas_vw;