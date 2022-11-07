# Databricks notebook source
#|---------------------------------------------------------------------|
#| Script to process Farm Assessment request from Supervisory          |
#|---------------------------------------------------------------------|
#| Change History. Type the change number at the end of each line      |
#| of code it applies to as follows:                                   |
#| 01I = change 01 inserted line,                                      |
#| 01D = change 01 deleted line.                                       |
#| Use change number 00 for a new program.                             |
#|----|------------|--------------------------------|------------------|
#| No | Date(mm/dd/yyyy) | Author | Request |                          |
#|----|------------|--------------------------------|------------------|
#| 000| 01/05/2021 | TCS Team                       |  Created         |
#|                                                                     |
#|                                                                     |
#| |-------------------------------------------------------------------|
#| | Python script used for the following                              |
#| | 1. Sending the input json request to CFT and get                  |
#|                      response json ans store it in the table        |
#|---------------------------------------------------------------------|

# COMMAND ----------

# DBTITLE 1,Import
import json,requests,pandas as pd
import pyspark.sql.functions as f
import datetime
import os
from flatten_json import flatten
from pandas import json_normalize
from pyspark.sql import Window
import pyspark.sql.types as t

# COMMAND ----------

# MAGIC %run "/Shared/CFT/utils/Setup"

# COMMAND ----------

# MAGIC %run "/Shared/CFT/utils/logging"

# COMMAND ----------

#Tables to be read
mapdf = spark.read.jdbc(url=jdbcUrl,table="CFT.FARM_MAPPING_CFT",properties = connectionProperties)
mapdf=mapdf.select(f.col('FARM_ID').alias('Farm_Identifier'),f.col('MARKET_ID').alias('market_id'),'MASKED_FARM_ID')

farmdf = spark.read.jdbc(url=jdbcUrl,table="CFT.FarmGeneralREQ",properties = connectionProperties)
farmdf=farmdf.select(f.col('farmid').alias('farm_id'),'Source',f.col('referenceyear'))


# COMMAND ----------

date = datetime.datetime.now()
FolderName = date.strftime("%d-%m-%Y")

# method to get json response
def json_generator(input_file,filename,emptydf):
  try:
    # posting the request json to CFT and getting the response 
    with open(input_file, 'r') as file:
        dairy_input = json.load(file)
    request = requests.post("https://app.coolfarmtool.org:/api/v1/dairy_product/calculate/", json=dairy_input, headers=HEADERS)
    dairy_output = request.json()
    # 200 = Error free json response
    if request.status_code == 200:
      folderpath = "/mnt/inbound/CFT/output/success/" 
      year = dairy_input[0]['milk_production']['reporting_year']
      # generating response in the json format
      output_generator(folderpath,dairy_output,dairy_input,filename)
      #Stroing the json into table
      df = table_creation(dairy_output,year)
    else:
      folderpath = "/mnt/inbound/CFT/output/error/"
      output_generator(folderpath,dairy_output,dairy_input,filename)
      df = emptydf.select(emptydf.columns)
    return df
    
  except:
    logger.error('Error in json_generator method',exc_info=True)
  
  
# method to store the json in storage account  
def output_generator(folderpath,dairy_output,dairy_input,filename): 
  try:
    folderpath = folderpath+FolderName+"/"
    filepath=folderpath+filename
    dbutils.fs.put(filepath,json.dumps(dairy_output,indent = 3),True)
  except:
    logger.error('Error in output_generator method',exc_info=True)

# COMMAND ----------

#flattening of json 
def flatten_json(nested_json: dict, exclude: list=['']) -> dict:
    """
    Flatten a list of nested dicts.
    """
    try:
      out = dict()
      def flatten(x: (list, dict, str), name: str='', exclude=exclude):
          if type(x) is dict:
              for a in x:
                  if a not in exclude:
                      flatten(x[a], f'{name}{a}_')
          elif type(x) is list:
              i = 0
              for a in x:
                  flatten(a, f'{name}{i}_')
                  i += 1
          else:
              out[name[:-1]] = x

      flatten(nested_json)
      return out
    except:
      logger.error('Error in flatten_json method',exc_info=True)



# COMMAND ----------

# creation of table from the json
def table_creation(dairy_output,year):
  try:
    #normalizing the json and flattening it into a dataframe
    df=json_normalize(data=dairy_output,record_path=['total_emissions'],meta=['farm'],errors='ignore')
    df1 = pd.DataFrame([flatten_json(dairy_output[0])])[['farm_farm_identifier','summary_emissions_total_0',
                                                         'summary_emissions_total_1','summary_emissions_per_fpcm_0','summary_emissions_per_fpcm_1']]
    for index,row in df.iterrows():
        df.loc[index,'farm'] = dairy_output[0]['farm']['farm_identifier']
    
    df =pd.merge(df1,df, left_on="farm_farm_identifier", right_on='farm')
    df.pop("farm")
    df.columns =['farm_id','emissions_total','summary_emissions_total_unit','emissions_per_fpcm',
                 'emissions_per_fpcm_unit','total_emissions_name','total_emissions_CO2','total_emissions_N2O',
                 'total_emissions_CH4','total_emissions_CO2e','total_CO2e_per_fpcm']
    df=df.drop(['summary_emissions_total_unit'],axis=1).drop(['emissions_per_fpcm_unit'],axis=1)
    df=df[['farm_id','emissions_per_fpcm','emissions_total','total_emissions_CH4',
           'total_emissions_CO2','total_emissions_N2O','total_emissions_name','total_emissions_CO2e','total_CO2e_per_fpcm']]
    
    #converting pandas df to pyspark
    df=spark.createDataFrame(df)
    df= df.withColumnRenamed('total_emissions_name','farm_activity_type').withColumnRenamed('total_CO2e_per_fpcm','emissions_per_activity')\
    .withColumn('farm_activity_type',f.when(f.col('farm_activity_type') == 'fertiliser','Fertilizer').otherwise(f.initcap(f.col('farm_activity_type'))))
    
    #Converting masked farm id to actual farmid 
    df = df.join(mapdf, df.farm_id == mapdf.MASKED_FARM_ID)
    df=df.withColumn('farm_id',f.lit(df.Farm_Identifier)).withColumn('referenceyear',f.lit(year)).drop('Farm_Identifier','MASKED_FARM_ID')
    df = df.join(farmdf, ['farm_id','referenceyear'])
    df = df.withColumn('Inserted_Timestamp',f.current_timestamp())
    df= df.select(df.columns).where(f.col('farm_activity_type')!= 'Transport')
    
    return df
  except:
    logger.error('Error in table_creation method',exc_info=True)
  

# COMMAND ----------

#creation of empty dataframe
schema = t.StructType([
t.StructField('farm_id',t.StringType(),True),
t.StructField('emissions_per_fpcm',t.DoubleType(),True),
t.StructField('emissions_total',t.DoubleType(),True),
t.StructField('total_emissions_CH4',t.DoubleType(),True),
t.StructField('total_emissions_CO2',t.DoubleType(),True),
t.StructField('total_emissions_N2O',t.DoubleType(),True),
t.StructField('farm_activity_type',t.StringType(),True),
t.StructField('total_emissions_CO2e',t.DoubleType(),True),
t.StructField('emissions_per_activity',t.DoubleType(),True),
t.StructField('market_id',t.StringType(),True),
t.StructField('referenceyear',t.IntegerType(),True),
t.StructField('Source',t.StringType(),True),
t.StructField('Inserted_Timestamp',t.TimestampType(),True)
  ])

flagdf=emptydf=spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)

# COMMAND ----------

try:
  #obtaining input json from the adls and storing it into table
  filelist = dbutils.fs.ls("/mnt/inbound/CFT/input/"+FolderName+"/")
  filedf=spark.createDataFrame(filelist,['files'])
  filedf = filedf.select(filedf.columns)
  filedf=filedf.withColumn("files",f.regexp_replace(f.col("files"), ":", ""))
  for row in filedf.collect():
    inputfile = row['files']
    print(inputfile)
    filename = os.path.basename(inputfile)
    inputfilepath="/"+inputfile
    df = json_generator(inputfilepath,filename,emptydf)
    flagdf = flagdf.unionByName(df)
  flagdf =flagdf.distinct()
  flagdf = flagdf.select('farm_id','market_id',f.col('referenceyear').cast(t.IntegerType()),'Source','Inserted_Timestamp','farm_activity_type',
                 f.col('emissions_per_fpcm').cast(t.DoubleType()),f.col('emissions_total').cast(t.DoubleType()),
                         f.col('total_emissions_CH4').cast(t.DoubleType()),
                 f.col('total_emissions_CO2').cast(t.DoubleType()),f.col('total_emissions_N2O').cast(t.DoubleType()),
                         f.col('total_emissions_CO2e').cast(t.DoubleType()),
                 f.col('emissions_per_activity').cast(t.DoubleType()))

  flagdf.write.option('truncate','true').jdbc(url=jdbcUrl, table="CFT.DairyProductOutput_STG", mode="overwrite", properties=connectionProperties)
except:
  logger.error('Error in output json creation',exc_info=True)



# COMMAND ----------

logger.info("output json generated")
logger.info("output table populated")
dbutils.notebook.exit('SUCCESS')