# Databricks notebook source
#|---------------------------------------------------------------------|
#| Script to anonymise id                                              |
#|---------------------------------------------------------------------|
#| Change History. Type the change number at the end of each line      |
#| of code it applies to as follows:                                   |
#| 01I = change 01 inserted line,                                      |
#| 01D = change 01 deleted line.                                       |
#| Use change number 00 for a new program.                             |
#|----|------------|--------------------------------|------------------|
#| No | Date(mm/dd/yyyy) | Author                   | Request          |
#|----|------------------|--------------------------|------------------|
#| 000 | 29/05/2021      | TCS Team                 |  Created         |
#|                                                                     |
#|
#| |-------------------------------------------------------------------|
#| | Python script used for the following                              |
#| | 1. Algorithm for masking an id based on input parameter           |
#|                                                                     |
#|---------------------------------------------------------------------|

# COMMAND ----------

# DBTITLE 1,Import
import requests
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../utils/Setup"

# COMMAND ----------

# MAGIC %md #Def Anonymise
# MAGIC ######Given a farm id and country code, create an anonymised id

# COMMAND ----------

def anonymiseid(farmid,country):
  finalid=0
  try: 
      idarray=[]
      combinedid=f'{farmid}{country}'
      for element in combinedid :
          if( element.isdigit()):
            if(int(element) % 2) == 0:
              idarray.append(int(element)*2)
            else:
              idarray.append(int(element)*3)
          else:
            idarray.append(ord(element))
      finalid=int(''.join(str(i) for i in idarray))
      finalid= str(finalid)[::-1]
  except ValueError:
    print(f'Could not convert {farmid}{country} to an anonymyze.', sys.exc_info()[0])
  except:
    print("Unexpected error:", sys.exc_info()[0])
  return finalid

# COMMAND ----------

stgDF = spark.read.jdbc(url=jdbcUrl,table="CFT.CFT_INPUT_STG",properties = connectionProperties)
maskdf = stgDF.select(F.col('Database_Identifier').alias('MARKET_ID'),F.col('Farm_Identifier').alias('FARM_ID'))	
maskdf.dropDuplicates()

maskUDF = F.udf(anonymiseid, StringType())

maskdf2 = maskdf.withColumn("MASKED_FARM_ID", maskUDF("FARM_ID","MARKET_ID"))
maskdf=maskdf2.dropDuplicates()

maskdf.write.jdbc(url=jdbcUrl,table='CFT.FARM_MAPPING_CFT',mode='overwrite',properties=connectionProperties)