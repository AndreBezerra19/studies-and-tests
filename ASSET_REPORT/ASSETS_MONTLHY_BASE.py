# Databricks notebook source
# MAGIC %md #Base Report
# MAGIC Processo SQL para criação da base de dados utilizado no report de base da area de ativos

# COMMAND ----------

# MAGIC %sql
# MAGIC use prod_gbs_finance_latam.gold_data;
# MAGIC create or replace table ASSET_MONTHLY_BASE as
# MAGIC 
# MAGIC select
# MAGIC  current_timestamp() AS AT1_UPDATE,
# MAGIC  ASSET_BASE.*,
# MAGIC  concat (ASSET_BASE.AT1_COMPANY_CODE,
# MAGIC          ASSET_BASE.AT1_ASSET,
# MAGIC          ASSET_BASE.AT1_SUBNUMBER
# MAGIC          ) as AT1_CONSOLIDADO, -- une os valores da empresa, ativo subativos, para que nao seja verificado em linhas incorretas
# MAGIC   INV_NUMBER.AT2_INVENTORY_NUMBER as AT1_INV_NUMBER, -- tras a coluna com o valor no numero de inventario
# MAGIC   DKEY.AT4_DEPRECIATION_KEY as DKEY, -- tras a coluna com o valor no numero da chave de depreciação
# MAGIC 
# MAGIC   ZAAREM.AT5_COST_CTR, -- tras a coluna com o vcentro de custo
# MAGIC 
# MAGIC   ZAAREM.AT5_LOCATION AS AT1_LOCATION, -- tras a coluna com a localização do ativo
# MAGIC   ZAAREM.AT5_DEPSTART AS AT1_DEPSTART, -- tras a coluna com o valor de inicio da depreciação
# MAGIC   ZAAREM.AT5_USE AS AT1_USE,
# MAGIC   ZAAREM.AT5_PER AS AT1_PER,
# MAGIC   ZAAREM.AT5_EUL AS AT1_EUL,
# MAGIC   ZAAREM.AT5_ELP AS AT1_ELP,
# MAGIC   ZAAREM.AT5_ARUL AS AT1_ARUL,
# MAGIC   ZAAREM.AT5_ARULP AS AT1_ARULP,
# MAGIC 
# MAGIC   INV_NUMBER.AT2_TYPE_NAME as AT1_ESPECIE, -- tras a coluna com especie do ativo
# MAGIC 
# MAGIC   CASE 
# MAGIC         WHEN ASSET_BASE.AT1_CURRENT_APC >= 0 
# MAGIC             THEN 'No'
# MAGIC         WHEN AT1_CURRENT_APC < 0 AND (
# MAGIC                 charindex('PIS', ASSET_BASE.AT1_ASSET_DESCRIPTION) +
# MAGIC                 charindex('COFINS', ASSET_BASE.AT1_ASSET_DESCRIPTION) +
# MAGIC                 charindex('ARO', ASSET_BASE.AT1_ASSET_DESCRIPTION) +
# MAGIC                 charindex('MINA', ASSET_BASE.AT1_ASSET_DESCRIPTION)
# MAGIC             ) > 0
# MAGIC             THEN 'NA'
# MAGIC         WHEN ASSET_BASE.AT1_CURRENT_APC < 0 
# MAGIC             THEN 'YES'
# MAGIC         ELSE 'ERROR'
# MAGIC     END AS with_negative_Cost_Value, -- identificar ativos com valores negativos
# MAGIC 
# MAGIC   CASE
# MAGIC         WHEN ASSET_BASE.AT1_CURRENT_APC = 0 and 
# MAGIC              ASSET_BASE.AT1_COMPANY_CODE = 'BRP' AND 
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 521
# MAGIC             THEN 'NA' 
# MAGIC         WHEN ASSET_BASE.AT1_CURRENT_APC = 0
# MAGIC             THEN 'Yes'
# MAGIC         ELSE 'No'
# MAGIC     END AS COST_VALUE_0, -- identifica ativos com valor zerado
# MAGIC 
# MAGIC   case
# MAGIC      when DKEY.AT4_DEPRECIATION_KEY = '0000'
# MAGIC       then 'Yes'
# MAGIC --        when charindex('CMD', ZAAREM.AT5_ASSET_DESCRIPTION) <> 0
# MAGIC     when ZAAREM.AT5_LOCATION = '71C' or
# MAGIC          ZAAREM.AT5_LOCATION = 'MP370' or
# MAGIC          ZAAREM.AT5_LOCATION = '911B'
# MAGIC       then 'CMD'
# MAGIC      else 'No'
# MAGIC     END AS Without_depreciation, -- identifica ativos sem depreciar
# MAGIC 
# MAGIC   case
# MAGIC     when ASSET_BASE.AT1_COMPANY_CODE in ('BRP', 'ADN', 'PAM')
# MAGIC       then IF(ZAAREM.AT5_LOCATION = 'nan','No','Yes')
# MAGIC     when ASSET_BASE.AT1_COMPANY_CODE not in ('BRP', 'ADN', 'PAM')
# MAGIC       then IF(ZAAREM.AT5_PLNT = 'nan','No','Yes') 
# MAGIC     else "ERROR"
# MAGIC     end as With_location, -- Identifica ativos com ou sem localização
# MAGIC     
# MAGIC   IF(ASSET_BASE.AT1_CURRBKVAL = 0, 'Yes','No') as Without_remaining_useful_life, -- Identifica ativos com ou sem vida util restante
# MAGIC   IF(MAIN_ASSETS.AT1_ASSET is null, 'Yes', 'No') as Sub_asset_without_asset, -- Identifica sub ativos sem o ativo principal
# MAGIC 
# MAGIC   case 
# MAGIC     when IF(MAIN_ASSETS.AT1_ASSET is null, 'Yes', 'No') = 'Yes' then 'Yes'  -- busca atraves do ativo filho, se existe um ativo pai
# MAGIC     when CONSOLIDADO_CC.AT5_COST_CTR=ZAAREM.AT5_COST_CTR then 'No'  -- compara o CC do ativo pai com o ativo filho
# MAGIC     else "Yes"
# MAGIC   end as SUB_IN_DIFERENT_CC, -- Identifica subativos lançados em CC diferente do principal
# MAGIC   
# MAGIC --   IF(CATALOGO.AX1_CLASSE <> ASSET_BASE.AT1_ASSET_CLASS, 'NOT OK', 'OK') AS Wrong_class_deprec, -- Identifica ativos lançados na classe de depreciação incorreta
# MAGIC   case
# MAGIC     when CATALOGO.AX1_CLASSE = ASSET_BASE.AT1_ASSET_CLASS
# MAGIC       then "No"
# MAGIC     when CATALOGO.AX1_CLASSE <> ASSET_BASE.AT1_ASSET_CLASS
# MAGIC       then "Yes"
# MAGIC     when INV_NUMBER.AT2_TYPE_NAME is null
# MAGIC      then "No, to be confirmed"
# MAGIC     else "Yes"
# MAGIC     end as Wrong_class_deprec,
# MAGIC     
# MAGIC   case
# MAGIC     when ASSET_BASE.AT1_ASSET_CLASS = 101 or
# MAGIC          ASSET_BASE.AT1_ASSET_CLASS = 107 or
# MAGIC          ASSET_BASE.AT1_ASSET_CLASS = 112 or
# MAGIC          ASSET_BASE.AT1_ASSET_CLASS = 122 or
# MAGIC          ASSET_BASE.AT1_ASSET_CLASS = 160 or
# MAGIC          ASSET_BASE.AT1_ASSET_CLASS = 607
# MAGIC      then 'EXCLUIR'
# MAGIC      when ASSET_BASE.AT1_ASSET_CLASS = 110 and
# MAGIC           ASSET_BASE.AT1_COMPANY_CODE = 'ADN'
# MAGIC       then 'EXCLUIR'
# MAGIC       else 'MANTER'
# MAGIC     end as CLASS_EXCLUDED, -- Identifica classes que devem ser excluidas da analise
# MAGIC 
# MAGIC   CASE
# MAGIC         WHEN ASSET_BASE.AT1_ASSET_CLASS = 356 OR
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 601 OR
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 603 OR
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 704 OR
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 705 OR
# MAGIC              ASSET_BASE.AT1_ASSET_CLASS = 160
# MAGIC          THEN 'NA'
# MAGIC         WHEN INV_NUMBER.AT2_INVENTORY_NUMBER is NULL
# MAGIC          THEN 'No'
# MAGIC         ELSE 'Yes'
# MAGIC     END AS WITH_INV_NUMBER, -- Identifica ativos com ou sem numero de inventario
# MAGIC 
# MAGIC     case 
# MAGIC       when ASSET_BASE.AT1_ASSET_CLASS = 102
# MAGIC         then 'Class of land'
# MAGIC       when ASSET_BASE.AT1_ASSET_CLASS = 809 or
# MAGIC            ASSET_BASE.AT1_ASSET_CLASS = 811
# MAGIC         then 'Exhaustion'
# MAGIC       when ASSET_BASE.AT1_ASSET_CLASS = 521
# MAGIC         then 'First-fill'
# MAGIC       when ASSET_BASE.AT1_ASSET_CLASS = 160
# MAGIC         then 'Spare parts - Relining'
# MAGIC       else 'NA'
# MAGIC     end as Why_not_depreciation, -- Identifica motivo de nao estar depreciando
# MAGIC 
# MAGIC     CASE 
# MAGIC         WHEN ASSET_BASE.AT1_SUBNUMBER = 0 
# MAGIC           THEN 'No'       
# MAGIC         WHEN ZAAREM.AT5_ARUL > 
# MAGIC             (
# MAGIC                 SELECT 
# MAGIC                     FIRST(ZAA.AT5_ARUL)
# MAGIC                 FROM prod_gbs_finance_latam.bronze_data.rpa_asset_zaarem AS ZAA
# MAGIC                 WHERE 
# MAGIC                     ZAA.AT5_SNO = 0 AND
# MAGIC                     ZAA.AT5_ASSET = ASSET_BASE.AT1_ASSET AND
# MAGIC                     ZAA.AT5_COCD = ASSET_BASE.AT1_COMPANY_CODE
# MAGIC             ) 
# MAGIC           THEN if (ASSET_BASE.AT1_ASSET_CLASS in (313, 525) and ASSET_BASE.AT1_COMPANY_CODE = 'BRP', 'NA', 'Yes')                   
# MAGIC         ELSE 'No'
# MAGIC     END AS Sub_asset_with_Useful_life_maior_Main_asset_Useful_life, -- Identifica sub ativos com vida util maior que o ativo principal
# MAGIC     
# MAGIC     date_part('year',(TO_DATE(LEFT(ASSET_BASE.AT1_CAPITALIZED_ON, 10), 'dd/MM/yyyy'))) as Year_of_capitalization  -- Isola o ano da capitalização
# MAGIC 
# MAGIC 
# MAGIC from prod_gbs_finance_latam.bronze_data.rpa_asset_asset_base as ASSET_BASE
# MAGIC 
# MAGIC   left join prod_gbs_finance_latam.bronze_data.rpa_asset_inv_number as INV_NUMBER on -- Junta informações de numero de inventario
# MAGIC     ASSET_BASE.AT1_COMPANY_CODE = INV_NUMBER.AT2_COMPANY_CODE and
# MAGIC     ASSET_BASE.AT1_ASSET = INV_NUMBER.AT2_ASSET and
# MAGIC     ASSET_BASE.AT1_SUBNUMBER = INV_NUMBER.AT2_SUBNUMBER
# MAGIC     
# MAGIC   left join prod_gbs_finance_latam.bronze_data.rpa_asset_dkey as DKEY on -- Junta informações da chave de depreciação
# MAGIC     ASSET_BASE.AT1_COMPANY_CODE = DKEY.AT4_COMPANY_CODE and
# MAGIC     ASSET_BASE.AT1_ASSET = DKEY.AT4_ASSET and
# MAGIC     ASSET_BASE.AT1_SUBNUMBER = DKEY.AT4_SUBNUMBER
# MAGIC     
# MAGIC   left join prod_gbs_finance_latam.bronze_data.rpa_asset_zaarem as ZAAREM on -- Junta informações da tabela ZAAREM
# MAGIC     ASSET_BASE.AT1_COMPANY_CODE = ZAAREM.AT5_COCD and
# MAGIC     ASSET_BASE.AT1_ASSET = ZAAREM.AT5_ASSET and
# MAGIC     ASSET_BASE.AT1_SUBNUMBER = ZAAREM.AT5_SNO
# MAGIC     
# MAGIC   left join prod_gbs_finance_latam.bronze_data.rpa_asset_catalogo_de_ativos as CATALOGO on 
# MAGIC     CATALOGO.AX1_ESPECIE = INV_NUMBER.AT2_TYPE_NAME
# MAGIC   left JOIN (
# MAGIC         SELECT
# MAGIC             distinct 
# MAGIC             AT1_COMPANY_CODE,
# MAGIC             AT1_ASSET
# MAGIC 
# MAGIC         FROM prod_gbs_finance_latam.bronze_data.rpa_asset_asset_base as ASSET_BASE_sub
# MAGIC             WHERE AT1_SUBNUMBER = 0
# MAGIC     ) AS MAIN_ASSETS ON --tabela de centro de custos dos ativos principais
# MAGIC             MAIN_ASSETS.AT1_COMPANY_CODE = ASSET_BASE.AT1_COMPANY_CODE AND
# MAGIC             MAIN_ASSETS.AT1_ASSET = ASSET_BASE.AT1_ASSET
# MAGIC     LEFT JOIN (
# MAGIC       Select
# MAGIC         ASSET_BASE_2.AT1_COMPANY_CODE, 
# MAGIC         ASSET_BASE_2.AT1_ASSET,
# MAGIC         ASSET_BASE_2.AT1_SUBNUMBER,
# MAGIC         ZAAREM_2.AT5_COST_CTR
# MAGIC       from prod_gbs_finance_latam.bronze_data.rpa_asset_asset_base AS ASSET_BASE_2
# MAGIC       
# MAGIC         left join prod_gbs_finance_latam.bronze_data.rpa_asset_zaarem AS ZAAREM_2 on
# MAGIC           ASSET_BASE_2.AT1_COMPANY_CODE = ZAAREM_2.AT5_COCD and
# MAGIC           ASSET_BASE_2.AT1_ASSET = ZAAREM_2.AT5_ASSET and
# MAGIC           ASSET_BASE_2.AT1_SUBNUMBER = ZAAREM_2.AT5_SNO
# MAGIC       WHERE 
# MAGIC         ASSET_BASE_2.AT1_SUBNUMBER=0 
# MAGIC     ) AS CONSOLIDADO_CC ON
# MAGIC         CONSOLIDADO_CC.AT1_COMPANY_CODE = ASSET_BASE.AT1_COMPANY_CODE and
# MAGIC         CONSOLIDADO_CC.AT1_ASSET = ASSET_BASE.AT1_ASSET

# COMMAND ----------

