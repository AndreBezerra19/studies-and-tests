-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Depreciation Report

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Main table

-- COMMAND ----------

-- Default table that will receive data increment

select
  *
from
  prod_gbs_finance_latam.silver_assets.asset_total_depreciation

-- COMMAND ----------

-- create a temporary view with the data that needs to be included

CREATE or replace TEMPORARY VIEW Current_update as

select 
AT13_COMPANY_CODE as AX2_EMPRESA,
sum (AT13_DEPRECIATION) as AX2_VALOR,
AT13_PERIOD as AX2_PERIODO

from prod_gbs_finance_latam.bronze_data.rpa_asset_depreciation as DEPREC
where AT13_COMPANY_CODE is not null
group by AT13_COMPANY_CODE, AT13_PERIOD

-- COMMAND ----------

-- confirms the temporary view of the data

select
  *
from
  Current_update

-- COMMAND ----------

--MERGE THE UPDAQTE DATA INTO HISTORICAL DATA
USE prod_gbs_finance_latam.silver_assets;

MERGE INTO asset_total_depreciation AS a
  USING Current_update AS b
  ON date_format(b.AX2_PERIODO, 'MMyyyy') = date_format(a.AX2_PERIODO, 'MMyyyy') and
     b.AX2_EMPRESA = a.AX2_EMPRESA
  WHEN NOT MATCHED
    THEN INSERT *;

-- COMMAND ----------

