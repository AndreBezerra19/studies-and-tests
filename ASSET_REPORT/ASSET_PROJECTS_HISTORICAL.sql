-- Databricks notebook source
-- MAGIC %md ##Historico de projetos

-- COMMAND ----------

list 'abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0'

-- COMMAND ----------

USE prod_gbs_finance_latam.silver_assets;

-- COMMAND ----------

--CREATE A VIEW ON TOP OF ALL THE FILES TARGET TO INGEST

CREATE OR REPLACE TEMP VIEW asset_ax_historico_projetos
USING CSV
OPTIONS (
  path = "abfss://rpa-resources@hgbsprodgbsflastorage01.dfs.core.windows.net/asset_report_AST01C121P0/historico_projetos.csv",  --It is possible to use WILDCARD character "*"
  header = "true",
  delimiter = ";",
  inferSchema = "true",
  mode = "FAILFAST"  -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
);


select * from asset_ax_historico_projetos;

-- COMMAND ----------

use prod_gbs_finance_latam.silver_assets;
create or replace table ASSET_PROJECTS_historico as

select
   CASE 
     WHEN contains(asset_ax_historico_projetos.Project,'BR')       
       THEN 'BRP'
     WHEN contains(asset_ax_historico_projetos.Project,'HA')       
       THEN 'ADN'
     WHEN contains(asset_ax_historico_projetos.Project,'HC')       
       THEN 'HCO'
     WHEN contains(asset_ax_historico_projetos.Project,'CP')       
       THEN 'CPA'
     WHEN contains(asset_ax_historico_projetos.Project,'TESTGFG')       
       THEN 'HCO'
     WHEN contains(asset_ax_historico_projetos.Project,'GFB') or     
          contains(asset_ax_historico_projetos.Project,'GIB')       
       THEN 'HCO'
       else 'PAM'
  END AS COCD,
  Project,
  WBS,
  `Nº AUC` as AUC
from
  asset_ax_historico_projetos;

-- COMMAND ----------

