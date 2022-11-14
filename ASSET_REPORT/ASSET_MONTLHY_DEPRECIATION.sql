-- Databricks notebook source
-- MAGIC %md #Depreciation report

-- COMMAND ----------

use prod_gbs_finance_latam.gold_data;
create or replace table ASSET_MONTHLY_DEPRECIATION as
-- create or replace temporary view depreciacao as
select
  DEPREC.*,
  left(AT13_ASSET,3) as AT13_Class,
  CLASS.CLASSE_DESCRICAO as CLASS_DESCR,
  CLASS.CONTA as CONTA,
  CLASS.CONTA_DRECICAO as CONTA_DESCR
from
  prod_gbs_finance_latam.bronze_data.rpa_asset_depreciation as DEPREC
  
left join prod_gbs_finance_latam.silver_assets.ax_deprec_classe as CLASS on
  left(DEPREC.AT13_ASSET,3) = CLASS.CLASSE

-- COMMAND ----------

