# Databricks notebook source
# MAGIC %md
# MAGIC #AP Daily Report - ingest bronze data
# MAGIC 
# MAGIC This notebook is intended to capture the data delivered by RPA, and load it into the bronze database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries

# COMMAND ----------

import pandas as pd
import numpy as np
import glob

import os, shutil
from timeit import default_timer as timer
from datetime import datetime, timedelta

start = timer()

# COMMAND ----------

# MAGIC %md ## Variables

# COMMAND ----------

#Tabelas principais
path_materiais = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/FBL3N_MAT.csv' #FBL3N
path_servicos = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/FBL3N_SVC.csv' #FBL3N
path_fretes = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/MB51.csv' #MB51
path_AP = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/FBL1N.csv' #FBL1N
path_VIM = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/OPT_VIM_VA2.csv' #PT_VIM_VA2
path_cockpit = '/dbfs/mnt/rpa_files/AP_Process/bronze_data/ZWFIC_DISPLAY_BRA.csv' #ZWFIC_DISPLAY_BRA
path_ServF = '/dbfs/mnt/rpa_files/AP_Process/silver_data/Fornecedores_Serviços.xlsx' #Fornecedores de serviços

#Tabelas auxiliares
path_ax_cockpit = '/dbfs/mnt/rpa_files/AP_Process/Auxiliares/COCKPIT.XLSX' #Textos cockpit
path_ax_vim = '/dbfs/mnt/rpa_files/AP_Process/Auxiliares/Regras_VIM.xlsx' #regras do VIM
path_ax_mat = '/dbfs/mnt/rpa_files/AP_Process/Auxiliares/textos_padrão_materiais.xlsx' #texto de materiais
path_ax_serv = '/dbfs/mnt/rpa_files/AP_Process/Auxiliares/textos_padrão_serviço.xlsx' #textos de serviço
path_ax_frt = '/dbfs/mnt/rpa_files/AP_Process/Auxiliares/Transportadoras.xlsx' #lista de transportadoras


# COMMAND ----------

# MAGIC %md #ETL Process

# COMMAND ----------

# MAGIC %md ## Materiais

# COMMAND ----------

# assign variables
path = path_materiais

# load data from csv
materiais = pd.read_csv(path)

materiais.columns = map(str.upper, materiais.columns)
materiais = materiais.add_prefix('AP1_')
materiais.columns = materiais.columns.str.replace(' ', '_')
materiais.columns = materiais.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

materiais['AP1_DOCUMENT_DATE'] = pd.to_datetime(materiais['AP1_DOCUMENT_DATE'], format= '%m/%d/%Y %H:%M:%S')
materiais['AP1_POSTING_DATE'] = pd.to_datetime(materiais['AP1_POSTING_DATE'], format= '%m/%d/%Y %H:%M:%S')

materiais['AP1_TEXT'] = materiais['AP1_TEXT'].astype(str)
materiais['AP1_VENDOR'] = materiais['AP1_VENDOR'].astype(str)

materiais.drop_duplicates(subset=['AP1_REFERENCE'], inplace = True) 

# materiais.head()

# COMMAND ----------

# MAGIC %md ## Serviços

# COMMAND ----------

# assign variables
path = path_servicos

# load data from csv
servicos = pd.read_csv(path)

servicos.columns = map(str.upper, servicos.columns)
servicos = servicos.add_prefix('AP2_')
servicos.columns = servicos.columns.str.replace(' ', '_')
servicos.columns = servicos.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

servicos['AP2_POSTING_DATE'] = pd.to_datetime(servicos['AP2_POSTING_DATE'], format= '%m/%d/%Y %H:%M:%S')
servicos['AP2_VALUE_DATE'] = pd.to_datetime(servicos['AP2_VALUE_DATE'], format= '%m/%d/%Y %H:%M:%S')

servicos['AP2_TEXT'] = servicos['AP2_TEXT'].astype(str)
servicos['AP2_SENT_SHEET'] = servicos['AP2_SENT_SHEET'].astype(str)

# servicos.head()

# COMMAND ----------

#list of service vendors classification

# assign variables
path = path_ServF

# load data from csv
F_Serv = pd.read_excel(path)

F_Serv.columns = map(str.upper, F_Serv.columns)
F_Serv = F_Serv.add_prefix('AUX1_')
F_Serv.columns = F_Serv.columns.str.replace(' ', '_')
F_Serv.columns = F_Serv.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# F_Serv.head()
#F_Serv.dtypes

# COMMAND ----------

# MAGIC %md ##Fretes

# COMMAND ----------

# assign variables
path = path_fretes

# load data from csv
fretes = pd.read_csv(path)

fretes.columns = map(str.upper, fretes.columns)
fretes = fretes.add_prefix('AP3_')
fretes.columns = fretes.columns.str.replace(' ', '_')
fretes.columns = fretes.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

fretes['AP3_DOCUMENT_DATE'] = pd.to_datetime(fretes['AP3_DOCUMENT_DATE'], format= '%m/%d/%Y %H:%M:%S')
fretes['AP3_POSTING_DATE'] = pd.to_datetime(fretes['AP3_POSTING_DATE'], format= '%m/%d/%Y %H:%M:%S')

fretes['AP3_SUPPLIER'] = fretes['AP3_SUPPLIER'].fillna(0).astype(int)
fretes['AP3_PURCHASE_ORDER'] = fretes['AP3_PURCHASE_ORDER'].fillna(0).replace('*', '0').astype(int)

fretes.head()

# COMMAND ----------

# MAGIC %md ##VIM

# COMMAND ----------

# assign variables
path = path_VIM

# load data from csv
VIM = pd.read_csv(path)

VIM.columns = map(str.upper, VIM.columns)
VIM = VIM.add_prefix('AP4_')
VIM.columns = VIM.columns.str.replace(' ', '_')
VIM.columns = VIM.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)
VIM['AP4_EXCEPTION_REASON'] = VIM['AP4_EXCEPTION_REASON'].str.replace(r'Brazil - ', '')

VIM['AP4_DOCUMENT_DATE'] = pd.to_datetime(VIM['AP4_DOCUMENT_DATE'], format= '%m/%d/%Y %H:%M:%S')
VIM['AP4_POSTING_DATE'] = pd.to_datetime(VIM['AP4_POSTING_DATE'], format= '%m/%d/%Y %H:%M:%S')

# VIM.head()

# COMMAND ----------

# MAGIC %md ##Cockpit

# COMMAND ----------

# assign variables
path = path_cockpit

# load data from csv
Cockpit = pd.read_csv(path)

Cockpit.columns = map(str.upper, Cockpit.columns)
Cockpit = Cockpit.add_prefix('AP5_')
Cockpit.columns = Cockpit.columns.str.replace(' ', '_')
Cockpit.columns = Cockpit.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# Cockpit.head()

# COMMAND ----------

# MAGIC %md ##FBL1N

# COMMAND ----------

# assign variables
path = path_AP

# load data from csv
AP = pd.read_csv(path)

AP.columns = map(str.upper, AP.columns)
AP = AP.add_prefix('AP6_')
AP.columns = AP.columns.str.replace(' ', '_')
AP.columns = AP.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)
AP['AP6_NET_DUE_DATE'] = pd.to_datetime(AP['AP6_NET_DUE_DATE'], format= '%m/%d/%Y %H:%M:%S')
# AP['AP6_NET_DUE_DATE'] = AP['AP6_NET_DUE_DATE'].astype('datetime64[ns]')

# AP.head()

# COMMAND ----------

# MAGIC %md ##Auxiliares

# COMMAND ----------

# assign variables
path = path_ax_mat

# load data from csv
AX_MAT = pd.read_excel(path)

AX_MAT.columns = map(str.upper, AX_MAT.columns)
AX_MAT = AX_MAT.add_prefix('AX1_')
AX_MAT.columns = AX_MAT.columns.str.replace(' ', '_')
AX_MAT.columns = AX_MAT.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

# AX_MAT.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Service vendors classification

# COMMAND ----------

# assign variables
path = path_ax_serv

# load data from csv
AX_SRV = pd.read_excel(path)

AX_SRV.columns = map(str.upper, AX_SRV.columns)
AX_SRV = AX_SRV.add_prefix('AX2_')
AX_SRV.columns = AX_SRV.columns.str.replace(' ', '_')
AX_SRV.columns = AX_SRV.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)

AX_SRV.columns = AX_SRV.columns.str.replace('AX2_TEXTO_PADRO__MATERIAIS', 'AX2_TEXTO_PADRO_SERVIÇOS')

# AX_SRV.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Freight providers list

# COMMAND ----------

# assign variables
path = path_ax_frt

# load data from csv
AX_FRT = pd.read_excel(path)

AX_FRT.columns = map(str.upper, AX_FRT.columns)
AX_FRT = AX_FRT.add_prefix('AX3_')
AX_FRT.columns = AX_FRT.columns.str.replace(' ', '_')
AX_FRT.columns = AX_FRT.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)


# AX_FRT.head()

# COMMAND ----------

# MAGIC %md #Prepare formatting for Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##Standardization

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

# MAGIC %md
# MAGIC ##Convert dataframes

# COMMAND ----------

spark_materiais = pandas_to_spark(materiais)
spark_servicos = pandas_to_spark(servicos)
spark_fretes = pandas_to_spark(fretes)
spark_VIM = pandas_to_spark(VIM)
spark_AP = pandas_to_spark(AP)
spark_CP = pandas_to_spark(Cockpit)
spark_F_Serv = pandas_to_spark(F_Serv)

#Auxiliares
spark_AX_MAT = pandas_to_spark(AX_MAT)
spark_AX_SRV = pandas_to_spark(AX_SRV)
spark_AX_FRT = pandas_to_spark(AX_FRT)

# COMMAND ----------

# MAGIC %md ## Create temp tables

# COMMAND ----------

spark_materiais.createOrReplaceTempView("MAT")
spark_servicos.createOrReplaceTempView("SERV")
spark_fretes.createOrReplaceTempView("FRT")
spark_VIM.createOrReplaceTempView("VIM")
spark_AP.createOrReplaceTempView("AP")
spark_CP.createOrReplaceTempView("CP")
spark_AX_MAT.createOrReplaceTempView("AX_MAT")
spark_AX_SRV.createOrReplaceTempView("AX_SRV")
spark_AX_FRT.createOrReplaceTempView("AX_FRT")
spark_F_Serv.createOrReplaceTempView("F_Serv")

# COMMAND ----------

# MAGIC %md # SQL Process

# COMMAND ----------

# MAGIC %md ###Create bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC use prod_gbs_finance_latam.bronze_data;
# MAGIC 
# MAGIC create or replace table ap_daily_materiais as
# MAGIC   select * from MAT;
# MAGIC   
# MAGIC create or replace table ap_daily_servicos as
# MAGIC   select * from SERV;
# MAGIC   
# MAGIC create or replace table ap_daily_fretes as
# MAGIC   select * from FRT;
# MAGIC   
# MAGIC create or replace table ap_daily_vim as
# MAGIC   select * from VIM;
# MAGIC   
# MAGIC create or replace table ap_daily_fbl1n as
# MAGIC   select * from AP;
# MAGIC 
# MAGIC create or replace table ap_daily_cockpit as
# MAGIC   select * from CP;
# MAGIC 
# MAGIC create or replace table ap_daily_ax_mat as
# MAGIC   select * from AX_MAT;
# MAGIC 
# MAGIC create or replace table ap_daily_ax_serv as
# MAGIC   select * from AX_SRV;
# MAGIC 
# MAGIC create or replace table ap_daily_ax_frt as
# MAGIC   select * from AX_FRT;
# MAGIC   
# MAGIC create or replace table ap_daily_serv_vendors as
# MAGIC   select * from F_Serv;

# COMMAND ----------

# MAGIC %md #Process execution time

# COMMAND ----------

end = timer()
print(timedelta(seconds=end-start))
