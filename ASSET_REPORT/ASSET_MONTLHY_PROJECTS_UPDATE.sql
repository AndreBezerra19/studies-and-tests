-- Databricks notebook source
-- MAGIC %md #Asset montlhy projects update
-- MAGIC Esse codigo carrega os dados da KORLIST que ainda nao estão registrados na lista historico de ativos

-- COMMAND ----------

-- MAGIC %md ##Historico de projetos

-- COMMAND ----------

select
  *
from
  prod_gbs_finance_latam.silver_assets.asset_projects_historico

-- COMMAND ----------

-- MAGIC %md ##Korlist mensal

-- COMMAND ----------

create temporary view monthly_update as

select
  KL.AT15_COCD as COCD,
  CN43N.AT8_PROJECT_DEFINITION as Project,
  KL.AT15_WBS as WBS,
  KL.AT15_AUC as AUC
from
  prod_gbs_finance_latam.silver_assets.asset_korlist as KL
left join prod_gbs_finance_latam.bronze_data.rpa_asset_cn43n as CN43N on
  KL.AT15_COCD = CN43N.AT8_COMPANY_CODE and
  KL.AT15_WBS = CN43N.AT8_WBS_ELEMENT
where left(KL.AT15_AUC, 3) in ('107', '112', '122')

-- COMMAND ----------

select * from monthly_update

-- COMMAND ----------

-- MAGIC %md ##Update mensal

-- COMMAND ----------

--MERGE THE UPDAQTE DATA INTO HISTORICAL DATA

MERGE INTO prod_gbs_finance_latam.silver_assets.asset_projects_historico AS a
  USING monthly_update AS b
  ON concat(a.COCD,a.Project,a.WBS,a.AUC) = concat(b.COCD,b.Project,b.WBS,b.AUC)
  WHEN NOT MATCHED
    THEN INSERT *;