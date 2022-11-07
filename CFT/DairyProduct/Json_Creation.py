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
#| | 1. Creation of input Json from  CFT request tables                |
#|                                                                     |
#|---------------------------------------------------------------------|

# COMMAND ----------

# DBTITLE 1,Import
import json,requests,pandas as pd
from datetime import datetime
import pyspark.sql.types as t
import pyspark.sql.functions as f


# COMMAND ----------

# MAGIC %run "../utils/logging"

# COMMAND ----------

# MAGIC %run "../utils/DairyProduct_classes"

# COMMAND ----------

# MAGIC 
# MAGIC %run "../utils/Json_methods"

# COMMAND ----------

#Tables to be read
farmiddf= spark.read.jdbc(url=jdbcUrl,table='CFT.CFT_INPUT_STG',properties = connectionProperties)
farmiddf=farmiddf.select(f.col('Farm_Identifier').alias('farmid'),f.col('Database_Identifier').alias('Market_ID'),
                         f.col('Field_Value').alias('referenceyear')).where(f.trim(f.lower(f.col('Field_Key')))== 'reference year').distinct()

# COMMAND ----------

#reading table data
mapdf = spark.read.jdbc(url=jdbcUrl,table="CFT.FARM_MAPPING_CFT",properties = connectionProperties)
mapdf = mapdf.select('FARM_ID','MASKED_FARM_ID')

feeditemdf= spark.read.jdbc(url=jdbcUrl, table= "CFT.feeditem",properties = connectionProperties)
feeditemdf = feeditemdf.select('feedid','description','name')

farmdf = spark.read.jdbc(url=jdbcUrl,table='CFT.FarmGeneralREQ',properties=connectionProperties)
farmdf=farmdf.join(mapdf,farmdf.farmid == mapdf.FARM_ID)
farmdf=farmdf.join(farmiddf,['farmid','Market_ID','referenceyear'])
farmdf=farmdf.select(farmdf.columns).drop('FARM_ID','Farm_Identifier')
milkdf = spark.read.jdbc(url=jdbcUrl,table='CFT.MilkProductionREQ',properties=connectionProperties)
herddf = spark.read.jdbc(url=jdbcUrl,table='CFT.HerdREQ',properties=connectionProperties)
grazedf = spark.read.jdbc(url=jdbcUrl,table='CFT.GrazingREQ',properties=connectionProperties)
fertilizerdf = spark.read.jdbc(url=jdbcUrl,table='CFT.FertilizerREQ',properties=connectionProperties)
feeddf = spark.read.jdbc(url=jdbcUrl,table='CFT.feedcomponentREQ',properties=connectionProperties)
feeddf = feeddf.join(feeditemdf,feeddf.item_description == feeditemdf.description,'left_outer' ).distinct()
manuredf = spark.read.jdbc(url=jdbcUrl,table='CFT.ManureREQ',properties=connectionProperties)
beddingdf = manuredf.select('farmid','assessmentid','referenceyear','beddingtype','quantity','unit',
                            'Market_ID','Farm_Dim_Sk','Source','Category_ID').distinct()
manuredf = manuredf.select('farmid','assessmentid','referenceyear','herdsection','manuretype','allocation','inserteddate','processeddate',
                           'Market_ID','Reviewed','Validated','Comments','Farm_Dim_Sk','Source','Category_ID','ID')
energydf = spark.read.jdbc(url=jdbcUrl,table='CFT.DirectEnergyREQ',properties=connectionProperties)

# COMMAND ----------

# MAGIC %md # Convert CFT Table(Request) to Json Format(Request)

# COMMAND ----------

# DBTITLE 1,Convert Farm object to json format
# Checking the mandatory columns and generating the json
def farm_out(df,farmid,assessmentid):
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='farm'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return farm_result
      else:
        farm_obj=Farm(row)
        farm_result=farm_obj.__dict__
    return farm_result


# COMMAND ----------

# DBTITLE 1,Convert Farm General object to json format
# Checking the mandatory columns and generating the json
def general_out(df,farmid,assessmentid,marketid,refid,source,category_id):
  for row in df.collect():
    for i in df.columns:
        if row[i]== None:
          column= i
          name='general'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return general_result
    else:
      general_obj=FarmGeneral(row)
    general_result=general_obj.__dict__
  return general_result


# COMMAND ----------

# DBTITLE 1,Convert Milk Production object to json format
# Checking the mandatory columns and generating the json
def milk_out(df,farmid,assessmentid):
  if df.rdd.isEmpty():
    milk_result = {}
    df1= spark.createDataFrame([(farmid,assessmentid,'milk production activity is empty')],['farmid','assessmentid','reason'])\
    .withColumn('inserteddate',f.current_timestamp())
    df1.write.jdbc(url=jdbcUrl,table='CFT.error_data_json',mode='append',properties=connectionProperties)
  else:
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='milk production'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            milk_result = {}
            return milk_result
      else:
        milk_obj = Milk(row)
    milk_result=milk_obj.__dict__
  return milk_result


# COMMAND ----------

# DBTITLE 1,Convert Herd object to json format
# Checking the mandatory columns and generating the json
def herd_out(df,farmid,assessmentid):
  herd_result=[]
  if df.rdd.isEmpty():
    herd_result=[]
    df1= spark.createDataFrame([(farmid,assessmentid,'Herd activity is empty')],['farmid','assessmentid','reason'])\
    .withColumn('inserteddate',f.current_timestamp())
    df1.write.jdbc(url=jdbcUrl,table='CFT.error_data_json',mode='append',properties=connectionProperties)
  else:
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='herd'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return herd_result
      else:
        herd_obj=Herd(row)
        herd_result.append(herd_obj.__dict__)
  return herd_result


# COMMAND ----------

# DBTITLE 1,Convert Grazing object to json format
# Checking the mandatory columns and generating the json
def graze_out(df,farmid,assessmentid):
  graze_result=[]
  if df.rdd.isEmpty():
    graze_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
          if row[i]== None:
            column= i
            name='grazing section'
            a = mandatory(column,name,farmid,assessmentid)
            if a ==0:
              return graze_result
      else:
        graze_obj=Grazing(row)
        graze_result.append(graze_obj.__dict__)
  return graze_result


# COMMAND ----------

# DBTITLE 1,Convert Fertilizer object to json format
# Checking the mandatory columns and generating the json
def fertilizer_out(df,farmid,assessmentid):
  fertilizer_result=[]
  if df.rdd.isEmpty():
    fertilizer_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
        type_of_fertilizer= row['type']
        if (row[i]== None) and (type_of_fertilizer == 'Compose your own NPK') :
          column= i
          name='fertilizerNPK'
          a = mandatory(column,name,farmid,assessmentid)
        elif (row[i]== None) and (type_of_fertilizer != 'Compose your own NPK'):
          columns= i
          name='fertilizer'
          a = mandatory(columns,name,farmid,assessmentid)
          if a ==0:
             return fertilizer_result
      else:
        if row['type'] == 'Compose your own NPK':
          fertilizer_object = FertilizerNPK(row)
        else:
          fertilizer_object = Fertilizer(row)
      fertilizer_result.append(fertilizer_object.__dict__)
  return fertilizer_result


# COMMAND ----------

# DBTITLE 1,Convert Feed Components object to json format
# Checking the mandatory columns and generating the json
def feed_out(df,farmid,assessmentid):
  feed_result=[]
  if df.rdd.isEmpty():
    feed_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
        crop_columns = ['cropemissions','totalcropproduction']
        if (row[i]== None) and (i not in crop_columns):
          column= i
          name='feed components'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return feed_result
      else:
        if row['cropemissions']== None and row['totalcropproduction'] == None:
          feed_obj = Feed(row)
        else:
          feed_obj = CropFeed(row)
        feed_result.append(feed_obj.__dict__)
  return feed_result


# COMMAND ----------

# DBTITLE 1,Convert Manure object to json format
# Checking the mandatory columns and generating the json
def manure_out(df,farmid,assessmentid):
  manure_result=[]
  if df.rdd.isEmpty():
    manure_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='manure'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return manure_result
      else:
        manure_obj = Manure(row)
        manure_result.append(manure_obj.__dict__)
  return manure_result


# COMMAND ----------

# DBTITLE 1,Convert Bedding object to json format
# Checking the mandatory columns and generating the json
def bedding_out(df,farmid,assessmentid):
  bedding_result=[]
  df = df.na.drop()
  if df.rdd.isEmpty():
    bedding_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='bedding'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return bedding_result
      else:
        bedding_obj=Bedding(row)
        bedding_result.append(bedding_obj.__dict__)
  return bedding_result


# COMMAND ----------

# DBTITLE 1,Convert Direct Energy object to json format
# Checking the mandatory columns and generating the json
def energy_out(df,farmid,assessmentid):
  energy_result=[]
  if df.rdd.isEmpty():
    energy_result=[]
  else:
    for row in df.collect():
      for i in df.columns:
        if row[i]== None:
          column= i
          name='direct energy'
          a = mandatory(column,name,farmid,assessmentid)
          if a ==0:
            return energy_result
      else:
        energy_obj=Energy(row)
        energy_result.append(energy_obj.__dict__)
  return energy_result


# COMMAND ----------

def final_json(aid,fmid,farm_identifier,assment,marketid,source,category_id,folderpath):
  try:
    #farm
    fmdf=farmdf.select(farmdf.columns).where((farmdf.referenceyear == aid) & (farmdf.farmid == fmid)
                                             & (farmdf.Market_ID == marketid)).withColumn('maskedid',f.lit(farm_identifier))
    farm=farm_out(fmdf,fmid,assment)
    #farm general
    general=general_out(fmdf,fmid,assment,marketid,aid,source,category_id)
    #milk production
    mlkdf=milkdf.select(milkdf.columns).where((milkdf.referenceyear == aid) & (milkdf.farmid == fmid)& (milkdf.Market_ID == marketid))
    milk=milk_out(mlkdf,fmid,assment)
    #herd_sections
    hrddf=herddf.select(herddf.columns).where((herddf.referenceyear == aid) & (herddf.farmid == fmid) & (herddf.Market_ID == marketid))
    herd=herd_out(hrddf,fmid,assment)
    #grazing
    grzdf=grazedf.select(grazedf.columns).where((grazedf.referenceyear == aid) & (grazedf.farmid == fmid) & (grazedf.Market_ID == marketid))
    graze=graze_out(grzdf,fmid,assment)
    #fertilizer
    frtdf=fertilizerdf.select(fertilizerdf.columns).where((fertilizerdf.referenceyear == aid) & (fertilizerdf.farmid == fmid) 
                                                          & (fertilizerdf.Market_ID == marketid))
    fertilizer=fertilizer_out(frtdf,fmid,assment)
    #feed_components
    fedf=feeddf.select(feeddf.columns).where((feeddf.referenceyear == aid) & (feeddf.farmid == fmid) & (feeddf.Market_ID == marketid))
    feed=feed_out(fedf,fmid,assment)
    #manure
    mnrdf=manuredf.select(manuredf.columns).where((manuredf.referenceyear == aid) & (manuredf.farmid == fmid) & (manuredf.Market_ID == marketid))
    manure=manure_out(mnrdf,fmid,assment)
    #bedding
    beddf =  beddingdf.select(beddingdf.columns).where((beddingdf.referenceyear == aid) & (beddingdf.farmid == fmid) 
                                                                    & (beddingdf.Market_ID == marketid))
    bed=bedding_out(beddf,fmid,assment)
    #direct_energy
    dedf=energydf.select(energydf.columns).where((energydf.referenceyear == aid) & (energydf.farmid == fmid) & (energydf.Market_ID == marketid))
    energy=energy_out(dedf,fmid,assment)
    
    #result json
    result=[]
    result.append({'farm':farm,'general':general,'milk_production':milk,'herd_sections':herd,'grazing':graze,'fertilisers':fertilizer,'feed_components':feed,'manure':manure,
     'bedding':bed,'direct_energy':energy, 'transport':[] 
       })
    
    #storing the json in storage account
    FileName = str(marketid)+'_'+str(fmid)+'_'+str(aid)+".json"
    filepath=folderpath+FileName
    json_generated = json.dumps(result,indent=4)
    dbutils.fs.put(filepath,json_generated,True)
    
  except:
    logger.error('Error in generating input_json method',exc_info=True)

# COMMAND ----------

# DBTITLE 1,Generate final json
for row in farmdf.collect():
  aid=row['referenceyear']
  fmid= row['farmid']
  farm_identifier=row['MASKED_FARM_ID']
  assment=row['assessmentid']
  marketid=row['Market_ID']
  source = row['Source']
  category_id = row['Category_ID']
  date = datetime.now()
  FolderName = date.strftime("%d-%m-%Y") 
  folderpath = "/mnt/inbound/CFT/input/"+FolderName+"/"
  dbutils.fs.mkdirs(folderpath)
  print(folderpath)
  final_json(aid,fmid,farm_identifier,assment,marketid,source,category_id,folderpath)
  

# COMMAND ----------

logger.info("Input json Created")
dbutils.notebook.exit('SUCCESS')