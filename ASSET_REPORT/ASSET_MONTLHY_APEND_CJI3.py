# Databricks notebook source
# MAGIC %md # CJI3 complete data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Historical table

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *,
# MAGIC   concat(AT10_PERIOD, "-", AT10_FISCAL_YEAR) as AT10_KEY_PERIOD
# MAGIC from
# MAGIC   prod_gbs_finance_latam.silver_assets.asset_historico_cji3 as CJI3

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count (*) from prod_gbs_finance_latam.silver_assets.asset_historico_cji3

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *,
# MAGIC   concat(AT10_PERIOD,"-",AT10_FISCAL_YEAR) as AT10_KEY_PERIOD
# MAGIC from
# MAGIC   prod_gbs_finance_latam.bronze_data.rpa_asset_cji3

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count (*) from prod_gbs_finance_latam.bronze_data.rpa_asset_cji3

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   concat(b.AT10_FISCAL_YEAR,"-",b.AT10_PERIOD) as periodo,
# MAGIC   count(*)
# MAGIC from prod_gbs_finance_latam.bronze_data.rpa_asset_cji3 as b
# MAGIC group by concat(b.AT10_FISCAL_YEAR,"-",b.AT10_PERIOD)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge first informations

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history prod_gbs_finance_latam.silver_assets.asset_historico_cji3

# COMMAND ----------

# MAGIC %sql
# MAGIC select periodo from (
# MAGIC   select
# MAGIC     distinct concat(b.AT10_FISCAL_YEAR,"-",b.AT10_PERIOD) as periodo
# MAGIC   from
# MAGIC     prod_gbs_finance_latam.silver_assets.asset_historico_cji3 as b
# MAGIC )
# MAGIC order by periodo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --MERGE THE UPDAQTE DATA INTO HISTORICAL DATA
# MAGIC USE prod_gbs_finance_latam.silver_assets;
# MAGIC 
# MAGIC MERGE INTO asset_historico_cji3 AS a
# MAGIC   USING prod_gbs_finance_latam.bronze_data.rpa_asset_cji3 AS b
# MAGIC   ON concat(a.AT10_PERIOD,"-",A.AT10_FISCAL_YEAR) = concat(b.AT10_PERIOD,"-",b.AT10_FISCAL_YEAR)
# MAGIC   WHEN NOT MATCHED
# MAGIC     THEN INSERT *;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count (*) from prod_gbs_finance_latam.silver_assets.asset_historico_cji3