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
#| | 1. Methods to process dairy json                                  |
#|                                                                     |
#|---------------------------------------------------------------------|

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %run "./logging"

# COMMAND ----------

#method for pivoting the staging table
def pivoting(mappingdict,df,defaultcols,groupbycols,defaultvalues):
  try:
    df=transpose(mappingdict,df,groupbycols)
    zip_ = list(set(df.columns)-{x for x in defaultcols})
    cols = list(set(df.columns)-{x for x in defaultcols})
    
    # To find the empty values and fill them with the previous values from same Category_ID
    zip_.extend(defaultcols)
    df=df.select([c for c in zip_])
    
    # To find the missing_columns values  which are in mandatory fields and fill them with the default values
    df=missingcolumns_array(defaultvalues,df)
    df=df.select([c for c in zip_])

    #Exploding the pivoted table from arrays to string values
    df= explode_column(df,cols,defaultcols)
    df=df.withColumn('inserteddate',f.current_timestamp()).drop('new').distinct()
    return df 
  except:
    logger.error('Error in pivoting method',exc_info=True)
  

# COMMAND ----------

# updating the existing records with newly generated records
def updating_table(tabledf,df,Group_Columns):
  try:
    if not(tabledf.rdd.isEmpty()):
      #Union of table and dataframe and select the rows with recent date
      df = tabledf.unionByName(df,allowMissingColumns=True).drop('ID')
      flagdf=df.groupBy(Group_Columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator"),
                                              f.max('inserteddate'),f.max('processeddate'))
      flagdf=flagdf.distinct()
      df=df.join(flagdf,Group_Columns)
      df=df.select(df.columns).where((f.col('inserteddate') == f.col('max(inserteddate)')) 
                                   & (f.col('processeddate') == f.col('max(processeddate)'))).distinct().drop('max(inserteddate)',
                                                                                                              'max(processeddate)','Duplicate_indicator')
      
    return df
  except:
    logger.error('Error in updating_table method',exc_info=True)

# COMMAND ----------

# transposing the staging table
def transpose(mappingdict,df,groupbycols):
  try:
    pivotcolumn='Field_ID'
    aggcolumn='Field_Value'
    mapping_expr = f.create_map([f.lit(x) for x in chain(*mappingdict.items())])
    df=df.withColumn('Field_ID',mapping_expr[f.col('Field_ID')])
    
    #creation of transpose by pivoting
    df=df.groupBy(groupbycols).pivot(pivotcolumn).agg(f.collect_list(aggcolumn))
    df=df.withColumnRenamed('Farm_Identifier','farmid').withColumnRenamed('Assessment_Date','processeddate')\
    .withColumnRenamed('Sequence','assessmentid').withColumnRenamed('Database_Identifier','Market_ID')
    return df
  except:
    logger.error('Error in transpose method',exc_info=True)

# COMMAND ----------

#method to create the columns which are mandatory and not present fill them with default values
def validate_column(mappingvalues,df,default):
  try:
    for i in mappingvalues.values():
      if (i not in df.columns) & (i in default):
         df=df.withColumn(i,f.lit(default[i]))
    return df
  except:
    logger.error('Error in validate_column method',exc_info=True)
  

# COMMAND ----------

#method to explode the array values 
def explode_column(df,columns_zipped,defaultcols):
  try:
    cols = defaultcols + ['new']
    df= df.withColumn("new",f.arrays_zip(*columns_zipped))
    df= df.withColumn("new",f.explode("new"))
    new_cols = ["new."+c for c in columns_zipped]
    df = df.select([b for b in cols])
    for c,d in zip(columns_zipped,new_cols):
      df= df.withColumn(c,f.col(d))
    return df
  except:
    logger.error('Error in explode_column method',exc_info=True)


# COMMAND ----------

# method to fill the empty values to default values
def missingcolumns(defaultvalues,df):
  try:
    if not defaultvalues:
      for col_name in df.columns:
        df=df.withColumn(col_name,f.when((f.col(col_name).isNull()) | (f.col(col_name)==''),f.lit(None)).otherwise(f.col(col_name)))
    else:
      for col_name in defaultvalues:
        df=df.withColumn(col_name,f.when((f.col(col_name).isNull()) | (f.col(col_name)==''),f.lit(defaultvalues[col_name])).otherwise(f.col(col_name)))
    return df
  except:
     logger.error('Error in missingcolumns method',exc_info=True)

# COMMAND ----------

def missingcolumns_array(defaultvalues,df):
  try:
    for col_name in defaultvalues:
      if col_name in df.columns:
        df=df.withColumn(col_name,f.when(f.size(f.col(col_name))==0,f.array(f.lit(defaultvalues[col_name]))).otherwise(f.col(col_name)))
    return df
  except:
     logger.error('Error in missingcolumns_array method',exc_info=True)

# COMMAND ----------

#appending the feeditem table to the dataframe to use the item id's
def idconversion(equalitycolumn,df,itemdf):
  try:
      for column in equalitycolumn:
        df = df.join(itemdf, f.trim(f.col(column)) == f.trim(itemdf.description),'left_outer')
        df=df.withColumn(column,f.lit(df.name))
      return df
  except:
     logger.error('Error in idconversion method',exc_info=True)

# COMMAND ----------

#appending the unit table to the dataframe to use the unit id's
def unitconversion(columns,df,unitdf):
  try:
    for column in columns:
      df=df.withColumn(column,f.lower(f.col(column))).withColumn(column,f.regexp_replace(f.col(column), " ", ""))
      df = df.join(unitdf, f.col(column) == unitdf.unitmeasurename,'left_outer')
      df=df.withColumn(column,f.lit(df.unitmeasureid))
    return df
  except:
     logger.error('Error in unitconversion method',exc_info=True)

# COMMAND ----------

#filtering out the newer entries 
def oldentries(groupby_oldentries,df):
  try:
    flagdf=df.groupBy(groupby_oldentries).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator"),
                                              f.max('Assessment_Timestamp'),f.max('processeddate'))
    flagdf=flagdf.distinct()
    df=df.join(flagdf,groupby_oldentries)
    df=df.select(df.columns).where((f.col('Assessment_Timestamp') == f.col('max(Assessment_Timestamp)')) 
                                   & (f.col('processeddate') == f.col('max(processeddate)'))).distinct()
    return df
  except:
     logger.error('Error in oldentries method',exc_info=True)

# COMMAND ----------

#check for mandatory columns are empty
def mandatory(column,name,farmid,assessmentid):
  try:
      lists= searching(name)
      if column in lists:
        reason = f"{column} in {name} activity is empty"
        df1= spark.createDataFrame([(farmid,assessmentid,reason)],['farmid','assessmentid','reason']).withColumn('inserteddate',f.current_timestamp())
        df1.write.jdbc(url=jdbcUrl,table='CFT.error_data_json',mode='append',properties=connectionProperties)
        return 0
      else:
        return 1
  except:
       logger.error('Error in mandatory method',exc_info=True)

# COMMAND ----------

#searching method contains all the mandatory fields in activities
def searching(name):
  try:
    farm=['country','territory','climate','avgtempvalue','avgtempunit']
    general=['grazingareavalue','grazingareaunit']
    milk=['variety','reportingyear','datetime','datemonth','name','productdryvalue','productdryunit','fatcontent','protiencontent','protienmeasure']
    herd=['phase','animals','liveweightvalue','liveweightunit','soldanimals','soldweightvalue','soldweightunit','purchasedanimals','purchasedweightvalue','purchasedweightunit']
    grazing=['herd_section','days','hours','category','quality']
    fertilizerNPK=['type','production','applicationratevalue','applicationrateunit','rateofmeasure','nammoniapercentage',
                   'nnitricpercentage','nureapercentage','p2o5percentage','k2opercentage','ntotalpercentage']
    fertilizer=['type','production','applicationratevalue','applicationrateunit','rateofmeasure']
    feed=['herdsection','item','drymattervalue','drymatterunit']
    manure=['herdsection','type','allocation']
    bedding=['type','quantity','unit']
    energy =['source','category','usagevalue','usageunit']
    if name =='farm':
      return farm
    if name =='general':
      return general
    if name =='milk production':
      return milk
    if name =='herd':
      return herd
    if name =='grazing section':
      return grazing
    if name =='fertilizerNPK':
      return fertilizerNPK
    if name =='fertilizer':
      return fertilizer
    if name =='feed components':
      return feed
    if name =='manure':
      return manure
    if name =='bedding':
      return bedding
    if name =='direct energy':
      return energy
  except:
     logger.error('Error in searching method',exc_info=True)