# Databricks notebook source
# MAGIC %md
# MAGIC # Carga Base historica CJI3
# MAGIC Necessario fazer uma carga inicial, pois os indicadores que utilizar esta base, fazem uso do historico acumulado de lançamentos mostrados pela transação CJI3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
import glob

import os, shutil
from timeit import default_timer as timer
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read base files

# COMMAND ----------

# variables
period = datetime.now()
CJI3 = pd.read_csv('/dbfs/mnt/rpa_files/asset_report_AST01C121P0/CJI3_Historico.csv', delimiter = ';')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Treat

# COMMAND ----------

CJI3.columns = map(str.upper, CJI3.columns)
CJI3 = CJI3.add_prefix('AT10_')
CJI3.columns = CJI3.columns.str.replace(' ', '_')
CJI3.columns = CJI3.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)
CJI3.head()

# COMMAND ----------

# convert the values
CJI3['AT10_POSTING_DATE'] = pd.to_datetime(CJI3['AT10_POSTING_DATE'], format= '%d/%m/%Y')
CJI3['AT10_DOCUMENT_DATE'] = pd.to_datetime(CJI3['AT10_DOCUMENT_DATE'], format= '%d/%m/%Y')

CJI3['AT10_PURCHASING_DOCUMENT'] = CJI3['AT10_PURCHASING_DOCUMENT'].fillna(0).replace("'-1", "-1").astype(int)
CJI3['AT10_OFFSETTING_ACCOUNT'] = CJI3['AT10_OFFSETTING_ACCOUNT'].fillna(0).astype(int)
CJI3['AT10_FISCAL_YEAR'] = CJI3['AT10_FISCAL_YEAR'].astype(int)
CJI3['AT10_PERIOD'] = CJI3['AT10_PERIOD'].astype(int)
CJI3['AT10_COST_ELEMENT'] = CJI3['AT10_COST_ELEMENT'].astype(int)

# add new columns to the dataframe
CJI3['AT10_AUX_TIMESTAMP'] = period

# filter out NaN rows
CJI3 = CJI3[CJI3['AT10_PERIOD'].notnull()]

# del CJI3['AT10_UNNAMED_0']
CJI3.head()

# COMMAND ----------

# MAGIC %md ## Tranform Pandas df to Spark

# COMMAND ----------

from pyspark.sql.types import *

# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return DateType()
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

spark_CJI3 = pandas_to_spark(CJI3)

# migrar para SQL temp view 
spark_CJI3.createOrReplaceTempView("input_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC use prod_gbs_finance_latam.silver_assets;
# MAGIC create or replace table asset_historico_cji3 as
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   input_data

# COMMAND ----------

