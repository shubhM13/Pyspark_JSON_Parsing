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
#| 000 | 01/05/2021 | TCS Team                       |  Created     |  |
#| 
#|
#| |-------------------------------------------------------------------|
#| | Python script used for the following                              |
#| | 1. Transpose supervisory input to CFT request format and          |
#|         load into tables                                            |
#|---------------------------------------------------------------------|

# COMMAND ----------

# MAGIC %md # Transpose of CFT staging tables to CFT Request tables
# MAGIC ## Input Table
# MAGIC * `CFT.CFT_INPUT_STG`
# MAGIC 
# MAGIC ## Output Tables
# MAGIC * `CFT.FarmGeneralREQ` 
# MAGIC * `CFT.HerdREQ`
# MAGIC * `CFT.ManureREQ`
# MAGIC * `CFT.FeedcomponentsREQ`
# MAGIC * `CFT.MilkProductionREQ`
# MAGIC * `CFT.GrazingREQ`
# MAGIC * `CFT.FertilizerREQ`
# MAGIC * `CFT.DirectEnergyREQ`
# MAGIC * `CFT.CropProductionData`
# MAGIC 
# MAGIC ## Mapping
# MAGIC <table>
# MAGIC <tr>
# MAGIC   <td><b>Field_ID</b></td>
# MAGIC   <td><b>Atrribute Name</b></td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Country</td>
# MAGIC <td>1</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Territory (District)</td>
# MAGIC <td>2</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Climate</td>
# MAGIC <td>3</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Annual average temperature</td>
# MAGIC <td>4</td>
# MAGIC </tr>
# MAGIC <tr>
# MAGIC <td>Temperature unit of measure UoM</td>
# MAGIC <td>5</td>
# MAGIC </tr>
# MAGIC </table>

# COMMAND ----------

# DBTITLE 1,Import
import json,requests,pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
from itertools import chain
from pyspark.sql import Row
from collections import OrderedDict
import datetime

# COMMAND ----------

# MAGIC %run "../utils/logging"

# COMMAND ----------

# MAGIC %run "/Shared/CFT/common/AnonymizeIdentifier"

# COMMAND ----------

# MAGIC %run "../utils/Json_methods"

# COMMAND ----------

#Tables to be read
unitdf= spark.read.jdbc(url=jdbcUrl,table="CFT.unitmeasure",properties=connectionProperties)
unitdf = unitdf.select('unitmeasurename','unitmeasureid')

feeditemdf= spark.read.jdbc(url=jdbcUrl, table= "CFT.feeditem",properties = connectionProperties)
feeditemdf = feeditemdf.select('activity','feedid','description','name','certified')

farm_skdf = spark.read.jdbc(url = jdbcUrl, table = "MADCAP_DWH.FARM_DIM", properties = connectionProperties )
farm_skdf = farm_skdf.select(f.col('FARM_ID').alias('farmid'),f.col('MARKET_ID').alias('Market_ID'),f.col('FARM_DIM_SK').alias('Farm_Dim_Sk'))


df = spark.read.jdbc(url=jdbcUrl,table = "CFT.CFT_INPUT_STG",properties = connectionProperties)

# COMMAND ----------

# DBTITLE 1,Farm Object
#Farm_object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence','Category_ID','Assessment_Timestamp']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source']
defaultvalues={}
mapping = {'M1':'country','M2':'territory','M3':'climate','M4':'avgtempvalue','M5':'avgtempunit','M6':'referenceyear','M8':'Notes'}
category =frmcategory =97

#Get All the records from farm general.
farmdf= df[df['Category_Number'] == category]
if ((not(farmdf.rdd.isEmpty()))):
  
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  farmdf= pivoting(mapping,farmdf,defaultcols,groupbycols,defaultvalues)
  farmdf = validate_column(mapping,farmdf,defaultvalues)
  farmdf=missingcolumns(defaultvalues,farmdf)
  
  #Default average temperature values
  farmdf=farmdf.withColumn("climate",f.lower(f.col("climate"))).withColumn('avgtempvalue',f.when((f.col('avgtempvalue')=='') & (f.col("climate") == 'tropical'),18).when((f.col('avgtempvalue')=='') & (f.col("climate") == 'temperate'),10).otherwise(farmdf.avgtempvalue))
  
  
  #Convert units in text to integer to match with json format. 
  column=['avgtempunit']
  farmdf=unitconversion(column,farmdf,unitdf)
  
  #Filtering the old entries 
  farm_groupby_oldentries = ['farmid','country','referenceyear','Market_ID']
  farmdf = oldentries(farm_groupby_oldentries,farmdf)
  
  #Adding FarmDim_SK
  farmdf=farmdf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for grazingareavalue,grazingareaunit,Reviewed,Validated and Comments
  farmdf = farmdf.withColumn('grazingareavalue',f.lit(0)).withColumn('grazingareaunit',f.lit('ha')).withColumn('feedapproach',f.lit('dmi'))\
  .withColumn('processeddate',f.lit(f.col('Assessment_Timestamp'))).withColumn('Reviewed',f.lit(None))\
  .withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  farmdf=farmdf.select('farmid','assessmentid','referenceyear','country','territory','climate',
                       'avgtempunit',f.col('avgtempvalue').cast(t.IntegerType()),f.col('grazingareavalue').cast(t.IntegerType()),
                              f.col('grazingareaunit'),f.col('feedapproach'),'inserteddate','processeddate','Market_ID','Source',
                       f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                             f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')


# COMMAND ----------

# DBTITLE 1,Farm General
#general object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence','Category_ID','Assessment_Timestamp']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source']
defaultvalues={'feedapproach':'dmi','grazingareavalue':0,'grazingareaunit':'Hectares'}
mapping = {'M11':'grazingareavalue','M12':'grazingareaunit','M0':'feedapproach','M13':'referenceyear'}
category = 109

#Get All the records from farm general.
generaldf = df[df['Category_Number'] == category]
generaldf = generaldf[(generaldf['Field_ID']=='M11') | (generaldf['Field_ID']=='M13')]


if ((not(generaldf.rdd.isEmpty()))):
   #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  generaldf= pivoting(mapping,generaldf,defaultcols,groupbycols,defaultvalues)
  generaldf = validate_column(mapping,generaldf,defaultvalues)
  generaldf=missingcolumns(defaultvalues,generaldf)
  
   #Convert units in text to integer to match with json format. 
  column=['grazingareaunit']
  generaldf=unitconversion(column,generaldf,unitdf)
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','referenceyear','Market_ID']
  generaldf = oldentries(groupby_oldentries,generaldf)
  
  #Adding FarmDim_SK
  generaldf=generaldf.join(farm_skdf,['farmid','Market_ID'])
  
  #Joining farm and general dataframes 
  generaldf=generaldf.select('farmid','referenceyear',f.col('grazingareavalue').alias('grazingareavalue1').cast(t.IntegerType()),
                              f.col('grazingareaunit').alias('grazingareaunit1'),
                             f.col('feedapproach').alias('feedapproach1').cast(t.StringType()),'Market_ID','Source',
                             f.col('Farm_Dim_Sk').cast(t.LongType()))

  farmdf = farmdf.join(generaldf,['farmid','referenceyear','Market_ID','Source','Farm_Dim_Sk'],'left_outer').distinct()
  
  farmdf=farmdf.withColumn('grazingareavalue',f.when(f.col('grazingareavalue1').isNotNull(),
                                                     f.lit(f.col('grazingareavalue1'))).otherwise(f.col('grazingareavalue')))\
  .withColumn('grazingareaunit',f.when(f.col('grazingareaunit1').isNotNull(),
                                        f.lit(f.col('grazingareaunit1'))).otherwise(f.col('grazingareaunit')))\
  .withColumn('feedapproach',f.when(f.col('feedapproach1').isNotNull(),f.lit(f.col('feedapproach1'))).otherwise(f.col('feedapproach')))
  
  farmdf=farmdf.select('farmid','assessmentid','referenceyear','country','territory','climate',
                       'avgtempunit',f.col('avgtempvalue').cast(t.IntegerType()),f.col('grazingareavalue').cast(t.IntegerType()),
                              f.col('grazingareaunit'),f.col('feedapproach'),'inserteddate','processeddate','Market_ID','Source',
                       f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                             f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')

  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.FarmGeneralREQ",properties = connectionProperties)
  farmdf = updating_table(tabledf,farmdf,farm_groupby_oldentries)
  
  #Stroing the data in DBFS
  farmdf.write.format("parquet").mode('overwrite').saveAsTable('Farm_General')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Farm_General")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.FarmGeneralREQ',mode='overwrite',properties=connectionProperties)


# COMMAND ----------

# DBTITLE 1,Direct Energy
#DirectEnergy_object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence',
               'Category_ID','Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={'category' :1}
mapping = {'M0':'category','S1':'energysource','S2':'value','S3':'Unit_measure','M1':'referenceyear'}
category =115

#Get All the records from Direct Energy.
energydf = df[df['Category_Number'] == category]
if ((not(energydf.rdd.isEmpty()))):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  energydf= pivoting(mapping,energydf,defaultcols,groupbycols,defaultvalues)
  energydf = validate_column(mapping,energydf,defaultvalues)
  energydf=missingcolumns(defaultvalues,energydf)
  
  #Convert units in text to integer to match with json format. 
  column=['Unit_measure']
  energydf=unitconversion(column,energydf,unitdf)
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','energysource','referenceyear','Market_ID']
  energydf = oldentries(groupby_oldentries,energydf)
  
  #Adding FarmDim_SK
  energydf=energydf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  energydf=energydf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp')))\
  .withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  energydf=energydf.select('farmid','assessmentid','referenceyear','energysource','category',
                             f.col('value').alias('usagevalue').cast(t.IntegerType()),
                           f.col('Unit_measure').alias('usageunit'),'inserteddate','processeddate','Market_ID','Source',
                           f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                           f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')
  
  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.DirectEnergyREQ",properties = connectionProperties)
  energydf = updating_table(tabledf,energydf,groupby_oldentries)
  
  #Stroing the data in DBFS
  energydf.write.format("parquet").mode('overwrite').saveAsTable('Direct_Energy')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Direct_Energy")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.DirectEnergyREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Feed Components
#FeedComponents Object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date',
               'Sequence','Category_ID','Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={}
mapping = {'M2':'referenceyear','S2':'dry_matter_value','S3':'dry_matter_unit','S1':'item','S4':'Crop_emission_from_CFT','S5':'Total_Crop_Production'}
category = 101

#Get All the records from feed components.
feeddf = df[df['Category_Number'] == category]
if ((not(feeddf.rdd.isEmpty()))):
 #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  feeddf= pivoting(mapping,feeddf,defaultcols,groupbycols,defaultvalues)
  feeddf = validate_column(mapping,feeddf,defaultvalues)
  feeddf=missingcolumns(defaultvalues,feeddf)
  
  #Renaming item column to item_description
  feeddf=feeddf.withColumn('item_description',f.lit(feeddf.item)).withColumn("item",f.lower("item")).withColumn("item",
                                                                                                                f.regexp_replace(f.col("item"), " ",""))
  #Convert item in text to integer and certified to match with json format
  feeddf = feeddf.join(feeditemdf, feeddf.item == feeditemdf.description)
  feeddf=feeddf.withColumn('item',f.lit(feeddf.feedid))
  feeddf=feeddf.withColumn('certified',f.lit(feeddf.certified)).withColumn('certified',f.lower(f.col('certified')))\
    .withColumn('herdsection',f.col('Field_SubKey')).withColumn('herdsection',f.when(f.col('herdsection') =='Meat Calves','Meat calves')
                                                                .when(f.col('herdsection') =='Dry Cows','Dry cows').otherwise(f.col('herdsection')))

  #Convert units in text to integer to match with json format. 
  feeddf = feeddf.withColumn('dry_matter_unit',f.when(f.col('dry_matter_unit') == 'Kilogramos','kilograms').otherwise(f.col('dry_matter_unit')))
  column=['dry_matter_unit']
  feeddf=unitconversion(column,feeddf,unitdf)
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','herdsection','item','referenceyear','Market_ID']
  feeddf = oldentries(groupby_oldentries,feeddf)
  
  #Adding FarmDim_SK
  feeddf=feeddf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  feeddf=feeddf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp'))).withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None))\
  .withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  feeddf=feeddf.select('farmid','assessmentid','referenceyear',f.col('herdsection'),'item',
                       f.col('dry_matter_value').alias('drymattervalue').cast(t.DoubleType()),f.col('dry_matter_unit').alias('drymatterunit'),
                       f.col('certified'),f.col('Crop_emission_from_CFT').alias('cropemissions'),
                       f.col('Total_Crop_Production').alias('totalcropproduction'),'inserteddate','processeddate','Market_ID','Source',
                       f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                       f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID','item_description')
  
#   Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.feedcomponentREQ",properties = connectionProperties)
  feeddf = updating_table(tabledf,feeddf,groupby_oldentries)
  
  #Stroing the data in DBFS
  feeddf.write.format("parquet").mode('overwrite').saveAsTable('Feed_Component')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Feed_Component")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.feedcomponentREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Fertilizer
#Fertilizers object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence',
               'Category_ID','Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={}
mapping ={'M1':'type','M2':'production','M3':'fertilizeron','S1':'nammoniapercentage','S2':'nnitricpercentage','S3':'nureapercentage',
          'S4':'p2o5percentage','S5':'k2opercentage','M9':'application_rate_value','M10':'application_rate_unit',
          'M11':'grassland_size','M12':'grassland_unit','M13':'referenceyear','M14':'rateofmeasure'}
category = 109

#Get All the records from fertilizer.
fertilizerdf = df[df['Category_Number'] == category]

if ((not(fertilizerdf.rdd.isEmpty()))):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  fertilizerdf= pivoting(mapping,fertilizerdf,defaultcols,groupbycols,defaultvalues)
  fertilizerdf = validate_column(mapping,fertilizerdf,defaultvalues)
  fertilizerdf=missingcolumns(defaultvalues,fertilizerdf)
  
  #Convert units in text to integer to match with json format. 
  column=['application_rate_unit']
  fertilizerdf=unitconversion(column,fertilizerdf,unitdf)
  
  #Check for ComposeNPK Fields
  composeNPK_columns =['nammoniapercentage','nnitricpercentage','nureapercentage','p2o5percentage','k2opercentage','ntotalpercentage']
  for i in composeNPK_columns:
    if (i not in fertilizerdf.columns):
      fertilizerdf = fertilizerdf.withColumn(i,f.lit(None))
  
  #ntotalpercentage = nammoniapercentage + nntiricpercentage + nureapercentage
  fertilizerdf=fertilizerdf.withColumn('ntotalpercentage',f.lit(f.col('nammoniapercentage')+f.col('nnitricpercentage')+f.col('nureapercentage')))
  
  #Covert type to lowercase with keywords
  equalitycolumn = ['type']
  fertilizerdf = idconversion(equalitycolumn,fertilizerdf,feeditemdf)
  
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','type','referenceyear','Market_ID']
  fertilizerdf = oldentries(groupby_oldentries,fertilizerdf)
  
  #Covert rate of measure,type to lowercase with keywords
  fertilizerdf=fertilizerdf.withColumn('rateofmeasure',f.lower(f.col('rateofmeasure')))\
  .withColumn('rateofmeasure',f.regexp_replace(f.col('rateofmeasure'), " ", ""))
  fertilizerdf=fertilizerdf.withColumn('rateofmeasure',f.when(f.col('rateofmeasure') == 'unitsofp2o5(p2o5)','p2o5')
                                       .when(f.col('rateofmeasure') == 'unitsofk2o(k2o)','k2o')
                                       .when(f.col('rateofmeasure') == 'unitsofnitrogen(n)','n').otherwise(f.col('rateofmeasure')))
  
  #Adding FarmDim_SK
  fertilizerdf=fertilizerdf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  fertilizerdf=fertilizerdf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp')))\
  .withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  fertilizerdf=fertilizerdf.select('farmid','assessmentid','referenceyear','type','production',
                                   f.col('application_rate_value').alias('applicationratevalue').cast(t.DoubleType()),
                                     f.col('application_rate_unit').alias('applicationrateunit'),
                                     'rateofmeasure','inserteddate','processeddate',f.col('nammoniapercentage').cast(t.DoubleType()),
                                   f.col('nnitricpercentage').cast(t.DoubleType()),
                                   f.col('nureapercentage').cast(t.DoubleType()),
                                 f.col('p2o5percentage').cast(t.DoubleType()),
                                   f.col('k2opercentage').cast(t.DoubleType()),f.col('ntotalpercentage').cast(t.DoubleType()),'Market_ID','Source',
                                   f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                                   f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')

  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.FertilizerREQ",properties = connectionProperties)
  fertilizerdf = updating_table(tabledf,fertilizerdf,groupby_oldentries)
  
  #Stroing the data in DBFS
  fertilizerdf.write.format("parquet").mode('overwrite').saveAsTable('Fertilizer')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Fertilizer")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.FertilizerREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Grazing
#Grazing object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence',
               'Category_ID','Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={}
mapping = {'M1':'grassland_fertilisation','M2':'grassland_area','M3':'unit','M4':'referenceyear',
           'S1':'days','S2':'hours','S3':'category','S4':'quality'}
category = 100

#Get All the records from grazing.
grazedf = df[df['Category_Number'] == category]
if ((not(grazedf.rdd.isEmpty()))):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  grazedf= pivoting(mapping,grazedf,defaultcols,groupbycols,defaultvalues)
  grazedf = validate_column(mapping,grazedf,defaultvalues)
  grazedf=missingcolumns(defaultvalues,grazedf)
  
  #Convert category, quality in text to integer to match with json format. 
  grazedf=grazedf.withColumn('category',f.when(grazedf.category == 'Confined Pasture',2).when(grazedf.category == 'Rangeland / Rough',3)
                             .otherwise(grazedf.category))\
  .withColumn('quality',f.when(grazedf.quality == 'High',1).when(grazedf.quality == 'Low',2)).withColumn('herdsection',f.col('Field_SubKey'))
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','herdsection','referenceyear','Market_ID']
  grazedf = oldentries(groupby_oldentries,grazedf)
  
  #Adding FarmDim_SK
  grazedf=grazedf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  grazedf=grazedf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp')))\
  .withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  grazedf=grazedf.select('farmid','assessmentid','referenceyear','herdsection',f.col('days').cast(t.IntegerType()),
                         f.col('hours').cast(t.IntegerType()),f.col('category').cast(t.IntegerType()),f.col('quality').cast(t.IntegerType()),
                         'inserteddate','processeddate','Market_ID','Source',
                         f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                         f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')
  
  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.GrazingREQ",properties = connectionProperties)
  grazedf = updating_table(tabledf,grazedf,groupby_oldentries)
  
  #Stroing the data in DBFS
  grazedf.write.format("parquet").mode('overwrite').saveAsTable('Grazing')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Grazing")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.GrazingREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Herd
#herd object
groupbycols = ['Source','Farm_Identifier','Assessment_Date','Sequence',
               'Category_ID','Database_Identifier','Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols=['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
             'SubCategory_Number','Field_SubKey','Field_SubID']
mapping = {'M2':'referenceyear','M1':'unit','S1':'animals','S2':'liveweightvalue','S3':'soldanimals',
           'S4':'soldweightvalue','S5':'purchasedanimals','S6':'purchasedweightvalue'}
defaultvalues={'animals':0,'soldanimals':0,'purchasedanimals':0,'liveweightvalue':0,'soldweightvalue':0,'purchasedweightvalue':0}
category = 99

#Get All the records from herd.
herddf = df[df['Category_Number'] == category]

if ((not(herddf.rdd.isEmpty()))):
    #Pivot the values based 'UserCharValue_Identifier' column value. MApping details are given in the header section of the command
    herddf= pivoting(mapping,herddf,defaultcols,groupbycols,defaultvalues)
    herddf = validate_column(mapping,herddf,defaultvalues)
    herddf=missingcolumns(defaultvalues,herddf)
    
    herddf=herddf.withColumn('phase',f.col('Field_SubKey'))
    
    #overwrite phase column with keywords
    equalitycolumn = ['phase']
    herddf = idconversion(equalitycolumn,herddf,feeditemdf)

    #Convert units in text to integer to match with json format. 
    column=['unit']
    herddf=unitconversion(column,herddf,unitdf)
    
    #Filtering the old entries 
    groupby_oldentries = ['farmid','phase','referenceyear','Market_ID']
    herddf = oldentries(groupby_oldentries,herddf)
  
    #Adding FarmDim_SK
    herddf=herddf.join(farm_skdf,['farmid','Market_ID'])

    #Default values for Reviewed,Validated and Comments
    herddf=herddf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp')))\
    .withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
    
    #Selecting the Required fields in the dataframe
    herddf=herddf.select('farmid','assessmentid','referenceyear','phase',f.col('animals').cast(t.IntegerType()),
                         f.col('liveweightvalue').cast(t.IntegerType()),
                         f.col('soldanimals').cast(t.IntegerType()),
                         f.col('soldweightvalue').cast(t.IntegerType()),
                         f.col('purchasedanimals').cast(t.IntegerType()),f.col('purchasedweightvalue').cast(t.IntegerType()),
                         f.col('unit'),'inserteddate','processeddate','Market_ID','Source',
                         f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                         f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')
    
    #Reading the sql table and updating the incremental values
    tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.HerdREQ",properties = connectionProperties)
    herddf = updating_table(tabledf,herddf,groupby_oldentries)
    
    #Stroing the data in DBFS
    herddf.write.format("parquet").mode('overwrite').saveAsTable('Herd')
    
    #Copying DBFS data to SQL
    adf = spark.sql("select * from  Herd")
    adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.HerdREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Milk Production
#MilkProduction object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence','Category_ID','Assessment_Timestamp']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source']
defaultvalues={'datetime':'start','protienmeasure':1}
mapping = {'M1':'variety','M2':'datemonth','M3':'referenceyear','M4':'name','M5':'productdryvalue','M6':'productdryunit',
           'M7':'fatcontent','M8':'protiencontent','M9':'crudeprotein','M10':'usernotes','M0':'protienmeasure','s0':'datetime'}
category = 98

#Get All the records from milk production.
milkdf = df[df['Category_Number'] == category]
if ((not(milkdf.rdd.isEmpty()))):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  milkdf= pivoting(mapping,milkdf,defaultcols,groupbycols,defaultvalues)
  milkdf = validate_column(mapping,milkdf,defaultvalues)
  milkdf=missingcolumns(defaultvalues,milkdf)
  
   #Convert units in text to integer to match with json format. 
  column=['productdryunit']
  milkdf=unitconversion(column,milkdf,unitdf)
  
  #Converting reference month from string to number
  milkdf=milkdf.withColumn('datemonth',milkdf.datemonth.substr(1,3)).withColumn('datemonth',
                                                                                f.from_unixtime(f.unix_timestamp(f.col('datemonth'),'MMM'),'MM'))
  
  #Filtering the old entries 
  groupby_oldentries = ['farmid','variety','name','referenceyear','Market_ID']
  milkdf = oldentries(groupby_oldentries,milkdf)
  
  #Adding FarmDim_SK
  milkdf=milkdf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  milkdf=milkdf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp'))).withColumn('Reviewed',f.lit(None)).withColumn('Validated',f.lit(None))\
  .withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  milkdf=milkdf.select('farmid','assessmentid','referenceyear','variety','datetime',
                       f.col('datemonth').cast(t.IntegerType()),'name',f.col('productdryvalue').cast(t.DecimalType()),
                       f.col('productdryunit'),f.col('fatcontent').cast(t.DoubleType()),f.col('protiencontent').cast(t.DoubleType()),
                       f.col('protienmeasure').cast(t.IntegerType()),'inserteddate','processeddate','Market_ID','Source',
                       f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                       f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')

  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = 'CFT.MilkProductionREQ',properties = connectionProperties)
  milkdf = updating_table(tabledf,milkdf,groupby_oldentries)
  
  #Stroing the data in DBFS
  milkdf.write.format("parquet").mode('overwrite').saveAsTable('Milk_Production')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Milk_Production")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.MilkProductionREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Manure
#Manure object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence','Category_ID',
               'Assessment_Timestamp','SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp'
               ,'Source','SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={}
mapping = {'M1_1':'referenceyear','M1_2':'bedding_referenceyear','S1_1':'herdsection','S2_1':'manuretype','S3_1':'allocation',
                     'S1_2':'beddingtype','S2_2':'quantity','S3_2':'unit'}
category = 110

#Get All the records from manure.
manuredf = df[(df['Category_Number'] == category)]
manuredf = manuredf.withColumn('Field_ID',f.concat_ws('_', f.trim(manuredf.Field_ID), f.trim(manuredf.SubCategory_Number)))

if not(manuredf.rdd.isEmpty()):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  manuredf= pivoting(mapping,manuredf,defaultcols,groupbycols,defaultvalues)
  manuredf = validate_column(mapping,manuredf,defaultvalues)
  manuredf=missingcolumns(defaultvalues,manuredf)
  
  #Convert units in text to integer to match with json format. 
  column=['unit']
  manuredf=unitconversion(column,manuredf,unitdf)
  
  #overwrite phase column with keywords
  equalitycolumn = ['herdsection']
  manuredf = idconversion(equalitycolumn,manuredf,feeditemdf)
  
  #joining bedding and manure dataframes
  beddingdf = manuredf.select('Source','farmid','Market_ID','Category_ID','assessmentid','Assessment_Timestamp','beddingtype',
                              'quantity','unit','unitmeasurename','unitmeasureid',
                              f.col('bedding_referenceyear').alias('referenceyear')).distinct().na.drop(subset=['referenceyear'])

  manuredf = manuredf.select('Source','farmid','Market_ID','Category_ID','referenceyear','assessmentid','Assessment_Timestamp','herdsection',
                             'manuretype','allocation','inserteddate','processeddate')
  
  manuredf = manuredf.join(beddingdf,['Source','farmid','Market_ID','Category_ID','referenceyear','assessmentid','Assessment_Timestamp'],'left_outer')

  #Filtering the old entries 
  groupby_oldentries = ['farmid','herdsection','referenceyear','Market_ID']
  manuredf = oldentries(groupby_oldentries,manuredf)
  
  #Adding FarmDim_SK
  manuredf=manuredf.join(farm_skdf,['farmid','Market_ID'])
  
  #Default values for Reviewed,Validated and Comments
  manuredf=manuredf.withColumn('processeddate',f.lit(f.col('Assessment_Timestamp'))).withColumn('Reviewed',f.lit(None))\
  .withColumn('Validated',f.lit(None)).withColumn('Comments',f.lit(None))
  
  #Selecting the Required fields in the dataframe
  manuredf=manuredf.select('farmid','assessmentid','referenceyear','herdsection','manuretype',
                           f.col('allocation').cast(t.DecimalType()),'beddingtype',f.col('quantity').cast(t.IntegerType()),
                             f.col('unit'),'inserteddate','processeddate','Market_ID','Source',
                           f.col('Reviewed').cast(t.StringType()),f.col('Validated').cast(t.StringType()),
                           f.col('Comments').cast(t.StringType()),f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')

  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.ManureREQ",properties = connectionProperties)
  manuredf = updating_table(tabledf,manuredf,groupby_oldentries)
  
  #Stroing the data in DBFS
  manuredf.write.format("parquet").mode('overwrite').saveAsTable('Manure')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Manure")
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.ManureREQ',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

# DBTITLE 1,Total Crop Production
#MilkProduction object
groupbycols = ['Source','Database_Identifier','Farm_Identifier','Assessment_Date','Sequence','Category_ID','Assessment_Timestamp',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultcols = ['farmid','processeddate','assessmentid','Category_ID','Market_ID','Assessment_Timestamp','Source',
               'SubCategory_Number','Field_SubKey','Field_SubID']
defaultvalues={}
mapping = {'M1':'Total_Crop_Production','M2':'referenceyear','S1':'Produce_Crop','S2':'Total_Production(Tonnes)','S3':'Total_Emission(KGCO2e)'}
category = 108

#Get All the records from milk production.
cropdf = df[df['Category_Number'] == category]
if ((not(cropdf.rdd.isEmpty()))):
  #Pivot the values based 'Field_ID' column value. Mapping details are given in the header section of the command
  cropdf= pivoting(mapping,cropdf,defaultcols,groupbycols,defaultvalues)
  cropdf = validate_column(mapping,cropdf,defaultvalues)
  cropdf=missingcolumns(defaultvalues,cropdf)
                                                      
  #Filtering the old entries 
  groupby_oldentries = ['farmid','Produce_Crop','referenceyear','Market_ID']
  cropdf = oldentries(groupby_oldentries,cropdf)
  
  #Adding FarmDim_SK
  cropdf=cropdf.join(farm_skdf,['farmid','Market_ID'])
  
  #Selecting the Required fields in the dataframe
  cropdf=cropdf.select('farmid','referenceyear','Total_Crop_Production','Produce_Crop',
                       f.col('Total_Production(Tonnes)').alias('Total_Production'),f.col('Total_Emission(KGCO2e)').alias('Total_Emission'),
                       f.col('inserteddate').alias('Created_Timestamp'),'Assessment_Timestamp','Market_ID','Source',
                       f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')

  #Reading the sql table and updating the incremental values
  tabledf = spark.read.jdbc(url=jdbcUrl,table = "CFT.CropProductionData",properties = connectionProperties)
  tabledf = tabledf.withColumnRenamed('Reference_Year','referenceyear').withColumnRenamed('Farm_ID','farmid')\
            .withColumnRenamed('Created_Timestamp','inserteddate').withColumnRenamed('Assessment_Timestamp','processeddate')\
            .withColumnRenamed('Total_Production(Tonnes)','Total_Production').withColumnRenamed('Total_Emission(KGCO2e)','Total_Emission')
  cropdf = updating_table(tabledf,cropdf,groupby_oldentries)
  
  #Stroing the data in DBFS
  cropdf.write.format("parquet").mode('overwrite').saveAsTable('Crop_Production')
  
  #Copying DBFS data to SQL
  adf = spark.sql("select * from  Crop_Production")
  adf=adf.select(f.col('farmid').alias('Farm_ID'),f.col('referenceyear').alias('Reference_Year'),'Total_Crop_Production','Produce_Crop',
                       f.col('Total_Production').alias('Total_Production(Tonnes)'),f.col('Total_Emission').alias('Total_Emission(KGCO2e)'),
                       f.col('Created_Timestamp'),f.col('Assessment_Timestamp'),'Market_ID','Source',
                       f.col('Farm_Dim_Sk').cast(t.LongType()),'Category_ID')
  adf.write.option('truncate','true').jdbc(url=jdbcUrl,table='CFT.CropProductionData',mode='overwrite',properties=connectionProperties)

# COMMAND ----------

logger.info("request tables generated")
dbutils.notebook.exit('SUCCESS')