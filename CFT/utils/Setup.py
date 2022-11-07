# Databricks notebook source
# DBTITLE 1,Configuration
KEY = dbutils.secrets.get(scope = "glbllegenddbrideunokey", key="CFTReqKey")
HEADERS = {
    "Content-Type": "application/json",
    "X-Api-App-Authorization": KEY
}
jdbcUsername = dbutils.secrets.get(scope = "glbllegenddbrideunokey", key="SQLUser")
jdbcPassword = dbutils.secrets.get(scope = "glbllegenddbrideunokey", key="SQLpwd")
jdbcHostname = dbutils.secrets.get(scope = "glbllegenddbrideunokey", key="SQLHostName")
jdbcDatabase = dbutils.secrets.get(scope = "glbllegenddbrideunokey", key="SQLDBLD")
jdbcPort = 1433
DRIVER="com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : DRIVER
}