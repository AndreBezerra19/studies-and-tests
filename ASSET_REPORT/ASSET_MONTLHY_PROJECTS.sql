-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Projects Report

-- COMMAND ----------

use prod_gbs_finance_latam.gold_data;
create or replace table ASSET_MONTHLY_PROJECTS as

select
  PROJETOS.*,
  CN42N.AT9_NAME as Project_description,  -- Descrição dos projetos
  CN43N.AT8_NAME as Pep_description,  -- Descrição dos WBS
  if(LAST_INPUT.LAST_DATE is null, 'Não Existe',LAST_INPUT.LAST_DATE)  as LAST_INPUT_DATE,  -- Data do ultimo lançamento no WBS
  ASSET_CLASS.AT3_CURRBKVAL as Valor,  -- Valor de AUC
  CN43N.AT8_STATUS as Status,  -- Status do WBS
  CN42N.AT9_START_DATE as Start_date,  -- Data de inicio do projeto
  CN42N.AT9_FINISH_DATE as End_date,  -- Data de termino do projeto
  
  if (contains(PROJETOS.AUC, '107'), 'With AUC', 'Without AUC') as With_withou_auc,  -- informação de com ou sem AUC
  
  if (CN42N.AT9_FINISH_DATE < current_timestamp(), "Expired", "On time") as WBS_Closing_Status,  -- status de projetos expirados e no prazo

  CASE 
     WHEN datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) >= 0 and
          datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) <= 91
       THEN '0 to 90 days'
     WHEN datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) >= 91 and
          datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) <= 180
       THEN '91 to 180 days'
     WHEN datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) >= 181 and
          datediff(CURRENT_DATE(), LAST_INPUT.LAST_DATE) <= 360
            then '181 to 360 days'
       else 'more 360 days'
  END AS range_days_of_last_input,  -- Classificação dos projetos por data de ultimos lançamentos

  date_part('year',CN42N.AT9_START_DATE) as year_of_cap  --coluna com o ano da capitalização

from
  prod_gbs_finance_latam.silver_assets.asset_projects_historico as PROJETOS  -- Tabela pricipal de projetos, WBS e AUC
  
left join prod_gbs_finance_latam.bronze_data.rpa_asset_cn42n as CN42N on  -- Tabela que tras informações de descrição de projetos e datas inicio e fim
  PROJETOS.COCD = CN42N.AT9_COMPANY_CODE and
  PROJETOS.Project = CN42N.AT9_PROJECT_DEFINITION
  
left join prod_gbs_finance_latam.bronze_data.rpa_asset_cn43n as CN43N on  -- Tabela que tras informações de descrição e status do WBS
  PROJETOS.COCD = CN43N.AT8_COMPANY_CODE and
  PROJETOS.WBS = CN43N.AT8_WBS_ELEMENT
  
left join prod_gbs_finance_latam.bronze_data.rpa_asset_class as ASSET_CLASS on  -- Tabela que tras o valos do AUC
  PROJETOS.COCD = ASSET_CLASS.AT3_COMPANY_CODE and
  PROJETOS.AUC = ASSET_CLASS.AT3_ASSET
  
left join
   (
     select
        concat(WBS.AT10_COMPANY_CODE,'-', WBS.AT10_OBJECT) as WBS_KEY,
      MAX(WBS.AT10_POSTING_DATE) AS LAST_DATE
    from
      prod_gbs_finance_latam.silver_assets.asset_historico_cji3 AS WBS 
    group by
      concat(WBS.AT10_COMPANY_CODE,'-', WBS.AT10_OBJECT)
   ) as LAST_INPUT on 
   LAST_INPUT.WBS_KEY = concat(CN43N.AT8_COMPANY_CODE,'-',CN43N.AT8_WBS_ELEMENT)  -- Tabela que tras a data  do ultimo lançamento