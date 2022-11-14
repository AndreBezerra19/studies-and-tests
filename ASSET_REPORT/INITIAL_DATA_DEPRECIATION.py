# Databricks notebook source
# MAGIC %md
# MAGIC #Total depreciation table

# COMMAND ----------

# MAGIC %python
# MAGIC # import the libraries dependency
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import glob
# MAGIC 
# MAGIC import os, shutil
# MAGIC from timeit import default_timer as timer
# MAGIC from datetime import datetime, timedelta
# MAGIC from sqlalchemy import create_engine

# COMMAND ----------

# MAGIC %python
# MAGIC # assign variables
# MAGIC path_DTotal = '/dbfs/mnt/rpa_files/asset_report_AST01C121P0/historical_data/historico_dep_total.csv'
# MAGIC path = path_DTotal
# MAGIC 
# MAGIC # load data from csv
# MAGIC Dep_Total = pd.read_csv(path, delimiter = ";", decimal = ",", thousands = ".")
# MAGIC 
# MAGIC Dep_Total.columns = map(str.upper, Dep_Total.columns)
# MAGIC Dep_Total = Dep_Total.add_prefix('AX2_')
# MAGIC Dep_Total.columns = Dep_Total.columns.str.replace(' ', '_')
# MAGIC Dep_Total.columns = Dep_Total.columns.str.replace('[^A-Za-z_0-9]+', '', regex=True)
# MAGIC 
# MAGIC Dep_Total['AX2_VALOR'] = Dep_Total['AX2_VALOR'].astype(float)
# MAGIC 
# MAGIC Dep_Total['AX2_PERIODO'] = pd.to_datetime(Dep_Total['AX2_PERIODO'], format= '%d/%m/%Y')

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC # Auxiliar functions
# MAGIC def equivalent_type(f):
# MAGIC     if f == 'datetime64[ns]': return DateType()
# MAGIC     elif f == 'int64': return LongType()
# MAGIC     elif f == 'int32': return IntegerType()
# MAGIC     elif f == 'float64': return DoubleType()
# MAGIC     elif f == 'float32': return FloatType()
# MAGIC     else: return StringType()
# MAGIC 
# MAGIC def define_structure(string, format_type):
# MAGIC     try: typo = equivalent_type(format_type)
# MAGIC     except: typo = StringType()
# MAGIC     return StructField(string, typo)
# MAGIC 
# MAGIC # Given pandas dataframe, it will return a spark's dataframe.
# MAGIC def pandas_to_spark(pandas_df):
# MAGIC     columns = list(pandas_df.columns)
# MAGIC     types = list(pandas_df.dtypes)
# MAGIC     struct_list = []
# MAGIC     for column, typo in zip(columns, types): 
# MAGIC       struct_list.append(define_structure(column, typo))
# MAGIC     p_schema = StructType(struct_list)
# MAGIC     return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

# MAGIC %python
# MAGIC spark_DepTotal = pandas_to_spark(Dep_Total)
# MAGIC spark_DepTotal.createOrReplaceTempView("Total_Depreciation")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use prod_gbs_finance_latam.silver_assets;
# MAGIC create or replace table ASSET_total_depreciation as
# MAGIC 
# MAGIC select * from Total_Depreciation