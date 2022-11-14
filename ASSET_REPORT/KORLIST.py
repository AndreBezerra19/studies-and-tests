# Databricks notebook source
# MAGIC %md
# MAGIC #KORLIST - Organization and loading for SQL

# COMMAND ----------

# import the libraries dependency
import pandas as pd
import numpy as np
import glob

import os, shutil
from timeit import default_timer as timer
from datetime import datetime, timedelta

start = timer()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Variables

# COMMAND ----------

#Base files
path_ADN = '//dbfs/mnt/rpa_files/asset_report_AST01C121P0/KORLIST/Korlist ADN.CSV' #Korlist ADN
path_BRP = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/KORLIST/Korlist BRP.CSV' #Korlist BRP
path_PAM = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/KORLIST/Korlist PAM.CSV' #Korlist PAM
path_CPA = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/KORLIST/Korlist CPA.CSV' #Korlist CPA
path_HCO = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/KORLIST/Korlist HCO.CSV' #Korlist HCO

# COMMAND ----------

# MAGIC %md
# MAGIC ###ETL Process

# COMMAND ----------

# MAGIC %md
# MAGIC ####ADN

# COMMAND ----------

# assign variables
path = path_ADN
# load data from csv
ADN_Base = pd.read_csv(path, skiprows=4, delimiter='\t',encoding='cp1252')
# espiar dados
ADN_Base.head()
# selecionando apenas duas colunas
ADN_Base = ADN_Base[['Receiver', 'Shrt txt receivers']]
#Adicionando categoria
ADN_Base['categoria'] = ADN_Base['Receiver'].str[:3]
#Adicionando codigo
ADN_Base['codigo'] = ADN_Base['Receiver'].str[4:]
# isolando a company code para uma nova coluna somente para IMO
ADN_Base['CoCd'] = (ADN_Base[
  ADN_Base['categoria'] == 'FXA'
])['codigo'].str[:3]
# isolando AuC
ADN_Base['AUC'] = (ADN_Base[
  ADN_Base['categoria'] == 'FXA'
])['codigo'].str[4:11]
# isolando WBS
ADN_Base['WBS'] = (ADN_Base[
  ADN_Base['categoria'] == 'WBS'
])['codigo']
# Replicando valor de WBS para linhas
cols = ['WBS']
ADN_Base.loc[:,cols] = ADN_Base.loc[:,cols].ffill()
ADN_Base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ####BRP

# COMMAND ----------

# assign variables
path = path_BRP
# load data from csv
BRP_Base = pd.read_csv(path, skiprows=4, delimiter='\t',encoding='cp1252')
# espiar dados
BRP_Base.head()
# selecionando apenas duas colunas
BRP_Base = BRP_Base[['Receiver', 'Shrt txt receivers']]
#Adicionando categoria
BRP_Base['categoria'] = BRP_Base['Receiver'].str[:3]
#Adicionando codigo
BRP_Base['codigo'] = BRP_Base['Receiver'].str[4:]
# isolando a company code para uma nova coluna somente para IMO
BRP_Base['CoCd'] = (BRP_Base[
  BRP_Base['categoria'] == 'FXA'
])['codigo'].str[:3]
# isolando AuC
BRP_Base['AUC'] = (BRP_Base[
  BRP_Base['categoria'] == 'FXA'
])['codigo'].str[4:11]
# isolando WBS
BRP_Base['WBS'] = (BRP_Base[
  BRP_Base['categoria'] == 'WBS'
])['codigo']
# Replicando valor de WBS para linhas
cols = ['WBS']
BRP_Base.loc[:,cols] = BRP_Base.loc[:,cols].ffill()
BRP_Base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ####PAM

# COMMAND ----------

# assign variables
path = path_PAM
# load data from csv
PAM_Base = pd.read_csv(path, skiprows=4, delimiter='\t',encoding='cp1252')
# espiar dados
PAM_Base.head()
# selecionando apenas duas colunas
PAM_Base = PAM_Base[['Receiver', 'Shrt txt receivers']]
#Adicionando categoria
PAM_Base['categoria'] = PAM_Base['Receiver'].str[:3]
#Adicionando codigo
PAM_Base['codigo'] = PAM_Base['Receiver'].str[4:]
# isolando a company code para uma nova coluna somente para IMO
PAM_Base['CoCd'] = (PAM_Base[
  PAM_Base['categoria'] == 'FXA'
])['codigo'].str[:3]
# isolando AuC
PAM_Base['AUC'] = (PAM_Base[
  PAM_Base['categoria'] == 'FXA'
])['codigo'].str[4:11]
# isolando WBS
PAM_Base['WBS'] = (PAM_Base[
  PAM_Base['categoria'] == 'WBS'
])['codigo']
# Replicando valor de WBS para linhas
cols = ['WBS']
PAM_Base.loc[:,cols] = PAM_Base.loc[:,cols].ffill()
PAM_Base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ####CPA

# COMMAND ----------

# assign variables
path = path_CPA
# load data from csv
CPA_Base = pd.read_csv(path, skiprows=4, delimiter='\t',encoding='cp1252')
# espiar dados
CPA_Base.head()
# selecionando apenas duas colunas
CPA_Base = CPA_Base[['Receiver', 'Shrt txt receivers']]
#Adicionando categoria
CPA_Base['categoria'] = CPA_Base['Receiver'].str[:3]
#Adicionando codigo
CPA_Base['codigo'] = CPA_Base['Receiver'].str[4:]
# isolando a company code para uma nova coluna somente para IMO
CPA_Base['CoCd'] = (CPA_Base[
  CPA_Base['categoria'] == 'FXA'
])['codigo'].str[:3]
# isolando AuC
CPA_Base['AUC'] = (CPA_Base[
  CPA_Base['categoria'] == 'FXA'
])['codigo'].str[4:11]
# isolando WBS
CPA_Base['WBS'] = (CPA_Base[
  CPA_Base['categoria'] == 'WBS'
])['codigo']
# Replicando valor de WBS para linhas
cols = ['WBS']
CPA_Base.loc[:,cols] = CPA_Base.loc[:,cols].ffill()
CPA_Base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ####HCO

# COMMAND ----------

# assign variables
path = path_HCO
# load data from csv
HCO_Base = pd.read_csv(path, skiprows=4, delimiter='\t',encoding='cp1252')
# espiar dados
HCO_Base.head()
# selecionando apenas duas colunas
HCO_Base = HCO_Base[['Receiver', 'Shrt txt receivers']]
#Adicionando categoria
HCO_Base['categoria'] = HCO_Base['Receiver'].str[:3]
#Adicionando codigo
HCO_Base['codigo'] = HCO_Base['Receiver'].str[4:]
# isolando a company code para uma nova coluna somente para IMO
HCO_Base['CoCd'] = (HCO_Base[
  HCO_Base['categoria'] == 'FXA'
])['codigo'].str[:3]
# isolando AuC
HCO_Base['AUC'] = (HCO_Base[
  HCO_Base['categoria'] == 'FXA'
])['codigo'].str[4:11]
# isolando WBS
HCO_Base['WBS'] = (HCO_Base[
  HCO_Base['categoria'] == 'WBS'
])['codigo']
# Replicando valor de WBS para linhas
cols = ['WBS']
HCO_Base.loc[:,cols] = HCO_Base.loc[:,cols].ffill()
HCO_Base.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consolidar bases

# COMMAND ----------

Frames = [BRP_Base, ADN_Base, PAM_Base, HCO_Base, CPA_Base]
KORLIST = pd.concat(Frames)
KORLIST = KORLIST[KORLIST.categoria == 'FXA']

KORLIST = KORLIST [['CoCd', 'WBS', 'AUC', 'Shrt txt receivers']]
KORLIST = KORLIST.rename(columns = {'Shrt txt receivers':'Shrt_txt_receivers'})
KORLIST.head()

# COMMAND ----------

KORLIST.columns = map(str.upper, KORLIST.columns)
KORLIST = KORLIST.add_prefix('AT15_')
KORLIST.columns = KORLIST.columns.str.replace(' ', '_')
KORLIST.columns = KORLIST.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

KORLIST.head()

# COMMAND ----------

# MAGIC %md #To Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##Preparar formatação

# COMMAND ----------

from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return DateTime()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converter para Spark df

# COMMAND ----------

spark_korlist = pandas_to_spark(KORLIST)


# COMMAND ----------

# MAGIC %md ## Tabelas temporarias

# COMMAND ----------

spark_korlist.createOrReplaceTempView("KORLIST")

# COMMAND ----------

# MAGIC %sql
# MAGIC use prod_gbs_finance_latam.silver_assets;
# MAGIC create or replace table ASSET_KORLIST as
# MAGIC --insert overwrite ASSET_KORLIST
# MAGIC 
# MAGIC select * from KORLIST

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ASSET_KORLIST

# COMMAND ----------

end = timer()
print(timedelta(seconds=end-start))