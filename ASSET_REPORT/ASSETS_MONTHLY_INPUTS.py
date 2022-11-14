# Databricks notebook source
# MAGIC %md #ASSETS_MONTHLY_INPUTS
# MAGIC 
# MAGIC processos de criação de base de dados para atualização do report mensal do time de ativos em power bi

# COMMAND ----------

# MAGIC %md ## Variables

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

path_Base = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_01_S_ALR_87011990.csv' #Base de ativos
path_Inventory_Num = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_03_S_ALR_87011990.csv' #Inventory Number e Type Name
path_Asset_Class = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_10_S_ALR_87011990.csv' #Classe 107
path_DKey = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_13_S_ALR_87011990.csv' #Depreciation key
path_Deprec = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_14_S_ALR_87012936.csv' #Relatorio de Depreciação
path_KOB1 = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_12_KOB1.csv'
path_KOK5 = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_11_KOK5.csv'
path_CN43N = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_09_CN43N.csv'
path_CN42N = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_08_CN42N.csv'
path_CJI3 = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_07_CJI3.csv'
path_IH08 = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_06_IH08.csv'
path_IH06 = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_05_IH06.csv'
path_ZAAREM = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/bronze_data/BR_KPI_04_ZAAREM.csv'
path_Catalogo = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/Catálogo de Ativos.xlsx'


period = datetime.now()

# COMMAND ----------

# MAGIC %md ##ETL Process

# COMMAND ----------

# MAGIC %md ###Base de Ativos

# COMMAND ----------

# assign variables
path = path_Base

# load data from csv
Asset_Base = pd.read_csv(path)

Asset_Base.columns = map(str.upper, Asset_Base.columns)
Asset_Base = Asset_Base.add_prefix('AT1_')
Asset_Base.columns = Asset_Base.columns.str.replace(' ', '_')
Asset_Base.columns = Asset_Base.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

Asset_Base['AT1_ASSET'] = Asset_Base['AT1_ASSET'].fillna(0).astype(int).astype(str)
Asset_Base['AT1_SUBNUMBER'] = Asset_Base['AT1_SUBNUMBER'].fillna(0).astype(int).astype(str)

# Asset_Base.head()

# COMMAND ----------

# MAGIC %md ### Inventory Number

# COMMAND ----------

# assign variables
path = path_Inventory_Num

# load data from csv
Inventory_Num = pd.read_csv(path)

Inventory_Num.columns = map(str.upper, Inventory_Num.columns)
Inventory_Num = Inventory_Num.add_prefix('AT2_')
Inventory_Num.columns = Inventory_Num.columns.str.replace(' ', '_')
Inventory_Num.columns = Inventory_Num.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# Inventory_Num.head()

# COMMAND ----------

# MAGIC %md ###DKey

# COMMAND ----------

# assign variables
path = path_DKey

# load data from csv
DKey = pd.read_csv(path)

DKey.columns = map(str.upper, DKey.columns)
DKey = DKey.add_prefix('AT4_')
DKey.columns = DKey.columns.str.replace(' ', '_')
DKey.columns = DKey.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# DKey.head()

# COMMAND ----------

# MAGIC %md ###Asset class

# COMMAND ----------

# assign variables
path = path_Asset_Class

# load data from csv
Asset_Class = pd.read_csv(path)

Asset_Class.columns = map(str.upper, Asset_Class.columns)
Asset_Class = Asset_Class.add_prefix('AT3_')
Asset_Class.columns = Asset_Class.columns.str.replace(' ', '_')
Asset_Class.columns = Asset_Class.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

Asset_Class['AT3_ASSET'] = Asset_Class['AT3_ASSET'].fillna(0).astype(int).astype(str)
Asset_Class['AT3_SUBNUMBER'] = Asset_Class['AT3_SUBNUMBER'].fillna(0).astype(int).astype(str)

# Asset_Class.head()

# COMMAND ----------

# MAGIC %md ###Depreciação

# COMMAND ----------

# assign variables
path = path_Deprec

# load data from csv
Deprec = pd.read_csv(path)

Deprec.columns = map(str.upper, Deprec.columns)
Deprec = Deprec.add_prefix('AT13_')
Deprec.columns = Deprec.columns.str.replace(' ', '_')
Deprec.columns = Deprec.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

Deprec = pd.melt(Deprec, id_vars=['AT13_COMPANY_CODE','AT13_COST_CENTER','AT13_ASSET','AT13_SUBNUMBER','AT13_ASSET_DESCRIPTION','AT13_DEPRECIATION_CALCULATION_START_DATE'],
                        value_vars=['AT13_DEP_0122','AT13_DEP_0222','AT13_DEP_0322','AT13_DEP_0422','AT13_DEP_0522','AT13_DEP_0622',
                                    'AT13_DEP_0722','AT13_DEP_0822','AT13_DEP_0922','AT13_DEP_1022','AT13_DEP_1122','AT13_DEP_1222'],
                        var_name='AT13_PERIOD', value_name='AT13_DEPRECIATION')

Deprec['AT13_DEPRECIATION_CALCULATION_START_DATE'] = Deprec['AT13_DEPRECIATION_CALCULATION_START_DATE'].astype('datetime64[ns]')

Deprec['AT13_ASSET'] = Deprec['AT13_ASSET'].fillna(0).astype(int)
Deprec['AT13_SUBNUMBER'] = Deprec['AT13_SUBNUMBER'].fillna(0).astype(int)

Deprec['AT13_PERIOD'] = pd.to_datetime('01'+ Deprec['AT13_PERIOD'].str[9:13], format='%d%m%y', errors='ignore')



Deprec.head()

# COMMAND ----------

display(Deprec)

# COMMAND ----------

# MAGIC %md ###KOB1

# COMMAND ----------

# assign variables
path = path_KOB1

# load data from csv
KOB1 = pd.read_csv(path)

KOB1.columns = map(str.upper, KOB1.columns)
KOB1 = KOB1.add_prefix('AT6_')
KOB1.columns = KOB1.columns.str.replace(' ', '_')
KOB1.columns = KOB1.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# KOB1.head()

# COMMAND ----------

# MAGIC %md ###KOK5

# COMMAND ----------

# assign variables
path = path_KOK5

# load data from csv
KOK5 = pd.read_csv(path)

KOK5.columns = map(str.upper, KOK5.columns)
KOK5 = KOK5.add_prefix('AT7_')
KOK5.columns = KOK5.columns.str.replace(' ', '_')
KOK5.columns = KOK5.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

KOK5.rename(columns = {'AT7_PERSON_RESPONSIBLE_1':'AT7_PERSON_RESPONSIBLE', 
                         'AT7_PERSON_RESPONSIBLE_2':'AT7_PERSON_RESPONSIBLE1'}, 
              inplace = True)

# KOK5.head()

# COMMAND ----------

# MAGIC %md ###CN43N

# COMMAND ----------

# assign variables
path = path_CN43N

# load data from csv
CN43N = pd.read_csv(path)

CN43N.columns = map(str.upper, CN43N.columns)
CN43N = CN43N.add_prefix('AT8_')
CN43N.columns = CN43N.columns.str.replace(' ', '_')
CN43N.columns = CN43N.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# CN43N.head(1000)

# COMMAND ----------

# MAGIC %md ###CN42N

# COMMAND ----------

# assign variables
path = path_CN42N

# load data from csv
CN42N = pd.read_csv(path)

CN42N.columns = map(str.upper, CN42N.columns)
CN42N = CN42N.add_prefix('AT9_')
CN42N.columns = CN42N.columns.str.replace(' ', '_')
CN42N.columns = CN42N.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)
CN42N['AT9_START_DATE'] = CN42N['AT9_START_DATE'].astype('datetime64[ns]')
CN42N['AT9_FINISH_DATE'] = CN42N['AT9_FINISH_DATE'].astype('datetime64[ns]')

# CN42N.head()

# COMMAND ----------

# MAGIC %md ###CJI3

# COMMAND ----------

# assign variables
path = path_CJI3

# load data from csv
CJI3 = pd.read_csv(path)

CJI3.columns = map(str.upper, CJI3.columns)
CJI3 = CJI3.add_prefix('AT10_')
CJI3.columns = CJI3.columns.str.replace(' ', '_')
CJI3.columns = CJI3.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

CJI3['AT10_OBJECT'] = CJI3['AT10_OBJECT'].astype(str)
CJI3['AT10_DOCUMENT_DATE'] = pd.to_datetime(CJI3['AT10_DOCUMENT_DATE'], format= '%m/%d/%Y %H:%M:%S')
CJI3['AT10_POSTING_DATE'] = pd.to_datetime(CJI3['AT10_POSTING_DATE'], format= '%m/%d/%Y %H:%M:%S')
CJI3['AT10_TIME_OF_ENTRY'] = CJI3['AT10_TIME_OF_ENTRY'].astype('datetime64[ns]')

CJI3['AT10_PERIOD'] = CJI3['AT10_PERIOD'].fillna(0).astype(int)
CJI3['AT10_FISCAL_YEAR'] = CJI3['AT10_FISCAL_YEAR'].fillna(0).astype(int)

CJI3['AT10_AUX_TIMESTAMP'] = period

CJI3['AT10_COST_ELEMENT'] = CJI3['AT10_COST_ELEMENT'].fillna(0).astype(int).astype(str)
CJI3 = CJI3.loc[CJI3['AT10_COST_ELEMENT'] != '193100']

CJI3.head() 

# COMMAND ----------

# MAGIC %md ###IH08

# COMMAND ----------

# assign variables
path = path_IH08

# load data from csv
IH08 = pd.read_csv(path)

IH08.columns = map(str.upper, IH08.columns)
IH08 = IH08.add_prefix('AT11_')
IH08.columns = IH08.columns.str.replace(' ', '_')
IH08.columns = IH08.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# IH08.head()

# COMMAND ----------

# MAGIC %md ###IH06

# COMMAND ----------

# assign variables
path = path_IH06

# load data from csv
IH06 = pd.read_csv(path)

IH06.columns = map(str.upper, IH06.columns)
IH06 = IH06.add_prefix('AT12_')
IH06.columns = IH06.columns.str.replace(' ', '_')
IH06.columns = IH06.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# IH06.head()

# COMMAND ----------

# MAGIC %md ### ZAAREM

# COMMAND ----------

# assign variables
path = path_ZAAREM

# load data from csv
ZAAREM = pd.read_csv(path)

ZAAREM.columns = map(str.upper, ZAAREM.columns)
ZAAREM = ZAAREM.add_prefix('AT5_')
ZAAREM.columns = ZAAREM.columns.str.replace(' ', '_')
ZAAREM.columns = ZAAREM.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

ZAAREM['AT5_LOCATION'] = ZAAREM['AT5_LOCATION'].astype(str)
ZAAREM['AT5_PLNT'] = ZAAREM['AT5_PLNT'].astype(str) 
ZAAREM['AT5_ROOM'] = ZAAREM['AT5_ROOM'].astype(str)
ZAAREM['AT5_COST_CTR'] = ZAAREM['AT5_COST_CTR'].astype(str)
ZAAREM.rename(columns = {'AT5_CUMACQUISVAL_1':'AT5_CUMACQUISVAL', 
                         'AT5_CUMACQUISVAL_2':'AT5_CUMACQUISVAL1', 
                         'AT5_DEPFYS_1':'AT5_DEPFYS',
                         'AT5_DEPFYS_2':'AT5_DEPFYS1',
                         'AT5_BKVAL_FYS_1':'AT5_BKVAL_FYS',
                         'AT5_BKVAL_FYS_2':'AT5_BKVAL_FYS1'},
              inplace = True)

# ZAAREM.head()

# COMMAND ----------

ZAAREM['AT5_PLNT'].unique()

# COMMAND ----------

# MAGIC %md ###Catalogo de Ativos

# COMMAND ----------

# assign variables
path = path_Catalogo

# load data from csv
Catalogo = pd.read_excel(path)

Catalogo.columns = map(str.upper, Catalogo.columns)
Catalogo = Catalogo.add_prefix('AX1_')
Catalogo.columns = Catalogo.columns.str.replace(' ', '_')
Catalogo.columns = Catalogo.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# Catalogo.head()

# COMMAND ----------

# MAGIC %md ## To Spark
# MAGIC Preparar formatação padronizada

# COMMAND ----------

from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return DateType()
    elif f == '<M8[ns]': return DateType()
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

# MAGIC %md ###Converter Df para Spark format

# COMMAND ----------

spark_base_de_ativos = pandas_to_spark(Asset_Base)
spark_inventory_number = pandas_to_spark(Inventory_Num)
spark_dkey = pandas_to_spark(DKey)
spark_asset_class = pandas_to_spark(Asset_Class)
spark_Drepreciacao = pandas_to_spark(Deprec)
spark_KOB1 = pandas_to_spark(KOB1)
spark_KOK5 = pandas_to_spark(KOK5)
spark_CN43N = pandas_to_spark(CN43N)
spark_CN42N = pandas_to_spark(CN42N)
spark_CJI3 = pandas_to_spark(CJI3)
spark_IH08 = pandas_to_spark(IH08)
spark_IH06 = pandas_to_spark(IH06)
spark_ZAAREM = pandas_to_spark(ZAAREM)
spark_Catalogo = pandas_to_spark(Catalogo)

# COMMAND ----------

# MAGIC %md ###Temp tables

# COMMAND ----------

spark_base_de_ativos.createOrReplaceTempView("ASSET_BASE")
spark_inventory_number.createOrReplaceTempView("INV_NUMBER")
spark_dkey.createOrReplaceTempView("DKEY")
spark_asset_class.createOrReplaceTempView("ASSET_CLASS")
spark_Drepreciacao.createOrReplaceTempView("DEPREC")
spark_KOB1.createOrReplaceTempView("KOB1")
spark_KOK5.createOrReplaceTempView("KOK5")
spark_CN43N.createOrReplaceTempView("CN43N")
spark_CN42N.createOrReplaceTempView("CN42N")
spark_CJI3.createOrReplaceTempView("CJI3")
spark_ZAAREM.createOrReplaceTempView("ZAAREM")
spark_Catalogo.createOrReplaceTempView("CATALOGO")

# COMMAND ----------

# MAGIC %sql
# MAGIC use prod_gbs_finance_latam.bronze_data;
# MAGIC 
# MAGIC create or replace table rpa_asset_asset_base as
# MAGIC   select * from ASSET_BASE;
# MAGIC   
# MAGIC create or replace table rpa_asset_inv_number as
# MAGIC   select * from INV_NUMBER;
# MAGIC   
# MAGIC create or replace table rpa_asset_dkey as
# MAGIC   select * from DKEY;
# MAGIC   
# MAGIC create or replace table rpa_asset_class as
# MAGIC   select * from ASSET_CLASS;
# MAGIC   
# MAGIC create or replace table rpa_asset_kob1 as
# MAGIC   select * from KOB1;
# MAGIC   
# MAGIC create or replace table rpa_asset_kok5 as
# MAGIC   select * from KOK5;
# MAGIC   
# MAGIC create or replace table rpa_asset_depreciation as
# MAGIC   select * from DEPREC;
# MAGIC   
# MAGIC create or replace table rpa_asset_cn42n as
# MAGIC   select * from CN42N;
# MAGIC   
# MAGIC create or replace table rpa_asset_cn43n as
# MAGIC   select * from CN43N;
# MAGIC   
# MAGIC create or replace table rpa_asset_cji3 as
# MAGIC   select * from CJI3;
# MAGIC   
# MAGIC create or replace table rpa_asset_zaarem as
# MAGIC   select * from ZAAREM;
# MAGIC   
# MAGIC create or replace table rpa_asset_catalogo_de_ativos as
# MAGIC   select * from CATALOGO;