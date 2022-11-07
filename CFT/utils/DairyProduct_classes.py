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
#| | 1. classes to process dairy json                                  |
#|                                                                     |
#|---------------------------------------------------------------------|

# COMMAND ----------

# MAGIC %run "./Setup"

# COMMAND ----------

class Farm(object):
  def __init__(self,farm):
    self.country= farm[8]
    self.territory =farm[9]
    self.climate = farm[10]
    self.average_temperature={'value':farm[12],'unit':farm[11]}
    self.farm_identifier = farm[21]


# COMMAND ----------

class FarmGeneral(object):
  def __init__(self,general):
    self.grazing_area={'value':general[13],'unit':general[14]}
    self.feed_approach=general[15]

# COMMAND ----------

class Milk(object):
  def __init__(self,milk):
    self.variety = milk[8]
    self.reporting_year = int(milk[6])
    self.date_time = milk[9]
    self.date_month = milk[10]
    self.name = milk[11]
    self.product_dry ={'value':float(milk[12]),'unit':milk[13]}
    self.fat_content = milk[14]
    self.protein_content = milk[15]
    self.protein_measure = milk[16]

# COMMAND ----------

class Herd(object):
  def __init__(self,herd):
    self.phase = herd[8]
    self.animals = herd[9]
    self.live_weight = {'value':int(herd[10]),'unit':herd[15]}
    self.sold_animals = herd[11]
    self.sold_weight = {'value': int(herd[12]),'unit': herd[15]}
    self.purchased_animals = herd[13]
    self.purchased_weight = {'value':int(herd[14]),'unit':herd[15]}


# COMMAND ----------

class Grazing(object):
  def __init__(self,graze):
    self.herd_section = graze[8]
    self.days = graze[9]
    self.hours = graze[10]
    self.category = graze[11]
    self.quality = graze[12]


# COMMAND ----------

class FertilizerNPK(object):
  def __init__(self,fertilizer):
    self.type = fertilizer[8]
    self.production = fertilizer[9]
    self.custom_ingredients = {
      'n_ammonia_percentage': fertilizer[13],
      'n_nitric_percentage': fertilizer[14],
      'n_urea_percentage': fertilizer[15],
      'p2o5_percentage': fertilizer[16],
      'k2o_percentage' : fertilizer[17],
      'n_total_percentage' : fertilizer[18],
    }
    self.application_rate = {'value': fertilizer[10], 'unit': fertilizer[11]}
    self.rate_measure = fertilizer[12]

# COMMAND ----------

class Fertilizer(object):
  def __init__(self,fertilizer):
    self.type = fertilizer[8]
    self.production = fertilizer[9]
    self.application_rate = {'value': fertilizer[10], 'unit': fertilizer[11]}
    self.rate_measure = fertilizer[12]

# COMMAND ----------

class Feed(object):
  def __init__ (self,feed):
    self.herd_section = feed[8]
    self.item = feed[10]
    self.dry_matter = {'value': feed[11],'unit':feed[12]}
    self.certified = bool(feed[15])

# COMMAND ----------

class CropFeed(object):
  def __init__ (self,cropfeed):
    self.herd_section = cropfeed[8]
    self.crop_product = {'type': cropfeed[23],
                         'product_dry':{'value': cropfeed[14],'unit': 'tonne'},
                         'feed_type': cropfeed[10],
                         'emissions_total':{'value': cropfeed[13],'unit': 'kg CO2e'}}
    self.dry_matter = {'value':cropfeed[11],'unit':cropfeed[12]}
    self.certified = bool(cropfeed[15])

# COMMAND ----------

class Manure(object):
    def __init__(self,manure):
        self.herd_section = manure[3]
        self.type = manure[4]
        self.allocation = int(manure[5])

# COMMAND ----------

class Bedding(object):
  def __init__(self,bedding):
    self.type=bedding[3]
    self.quantity={'value':bedding[4],'unit':bedding[5]}

# COMMAND ----------

class Energy(object):
  def __init__(self,energy):
    self.source=energy[8]
    self.usage={'value':energy[10],'unit':energy[11]}
    self.category=energy[9]