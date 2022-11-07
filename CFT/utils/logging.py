# Databricks notebook source
import logging

file_date =  dbutils.widgets.get("Current_date")
print(file_date)
p_dir = '/tmp/'
p_filename = 'custom_log '+file_date+' .log'
p_logfile = p_dir + p_filename 
print(p_logfile)
# create logger with 'Custom_log'
logger = logging.getLogger('log4j')
logger.setLevel(logging.DEBUG) 
# create file handler which logs even debug messages
fh = logging.FileHandler(p_logfile,mode='a')
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh.setFormatter(formatter)
#ch (console handler)
ch.setFormatter(formatter)
#Clearing old frequent log information to ignore that.
if (logger.hasHandlers()):
     logger.handlers.clear()
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)