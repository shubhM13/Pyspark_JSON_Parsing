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
#| | 1. Running the notebook (Json_request) containing Transpose       |
#|                         of the input table                          |
#| | 2. Running the notebook (Json_Creation) containing creation       |
#|                         of request json                             |
#| | 3. Running the notebook (CFT_Json_Process) containing             |
#|                         json Response creation                      |
#|---------------------------------------------------------------------|

# COMMAND ----------

import time
import datetime

# COMMAND ----------

Current_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
print(Current_date)

# COMMAND ----------

# MAGIC %md ##Run for the Transposing of the Staging table

# COMMAND ----------

status = dbutils.notebook.run("/Shared/CFT/DairyProduct/Json_request",1800,{'Current_date':Current_date})
print(status)

# COMMAND ----------

# MAGIC %md ## Run for the Input JSON Creation

# COMMAND ----------

input_json_status = dbutils.notebook.run("/Shared/CFT/DairyProduct/Json_Creation",1800,{'Current_date':Current_date})
print(input_json_status)

# COMMAND ----------

# MAGIC %md ##Run for the Output Json creation and Storing in the table

# COMMAND ----------

output_json_status = dbutils.notebook.run("/Shared/CFT/DairyProduct/CFT_Json_Process",1800,{'Current_date':Current_date})
print(output_json_status)

# COMMAND ----------

folder="/mnt/inbound/CFT/Log_files/"+Current_date
file="/tmp/custom_log "+Current_date+" .log"
dbutils.fs.mv("file:"+file, folder+".log")