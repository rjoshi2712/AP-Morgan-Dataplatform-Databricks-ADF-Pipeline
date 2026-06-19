# Databricks notebook source
# #get the file name from the adf
# fileName = dbutils.widgets.get('fileName')
# #fileName = 'Product.csv'
# fileNameWithoutExt = fileName.split('.')[0]
# print(fileNameWithoutExt)
# COMMAND ----------

# Get the file name from ADF
dbutils.widgets.text("fileName", "")

fileName = dbutils.widgets.get("fileName")

# Product.csv -> Product
# Product.Fail.csv -> Product
fileNameWithoutExt = fileName.split(".")[0]

print("File name from ADF:", fileName)
print("Metadata file name:", fileNameWithoutExt)
# COMMAND ----------

import pyspark.sql.functions as F
#from datetime import datetme as dt

#Just change all the values here based on the resource name you have created in your environemnt and workspace.

sqlDbName = '<AZURE_SQL_DATABASE_NAME>'
dbUserName = '<AZURE_SQL_USERNAME>'
passwordKey = '<SQL_PASSWORD_SECRET_KEY>'
stgAccountSASTokenKey = '<STORAGE_SAS_TOKEN_SECRET_KEY>'
landingFileName =fileName #'Product'  #dbutils.widgets.get('Product')
databricksScopeName ='<DATABRICKS_SECRET_SCOPE>'
dbServer = '<AZURE_SQL_SERVER_NAME>'
dbServerPortNumber ='1433'
storageContainer ='<STORAGE_CONTAINER_NAME>'
storageAccount='<STORAGE_ACCOUNT_NAME>'
landingMountPoint ='/mnt'

# COMMAND ----------

# ADLS connection setup using SAS token and direct ABFSS paths

storageAccount = "<STORAGE_ACCOUNT_NAME>"
storageContainer = "<STORAGE_CONTAINER_NAME>"

databricksScopeName = "<DATABRICKS_SECRET_SCOPE>"
stgAccountSASTokenKey = "<STORAGE_SAS_TOKEN_SECRET_KEY>"

# Get SAS token from Databricks secret scope
sasToken = dbutils.secrets.get(
    scope=databricksScopeName,
    key=stgAccountSASTokenKey
)

# Remove leading ? if SAS token has it
if sasToken.startswith("?"):
    sasToken = sasToken[1:]

# Tell Spark to use SAS authentication for ADLS Gen2
spark.conf.set(
    f"fs.azure.account.auth.type.{storageAccount}.dfs.core.windows.net",
    "SAS"
)

spark.conf.set(
    f"fs.azure.sas.token.provider.type.{storageAccount}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)

spark.conf.set(
    f"fs.azure.sas.fixed.token.{storageAccount}.dfs.core.windows.net",
    sasToken
)

# Direct ADLS paths
landingPath  = f"abfss://{storageContainer}@{storageAccount}.dfs.core.windows.net/landing/"
stagingPath  = f"abfss://{storageContainer}@{storageAccount}.dfs.core.windows.net/staging/"
rejectedPath = f"abfss://{storageContainer}@{storageAccount}.dfs.core.windows.net/rejected/"

print("ADLS connection configured successfully")
print("Landing path:", landingPath)
print("Staging path:", stagingPath)
print("Rejected path:", rejectedPath)
# COMMAND ----------

#connect to Azure SQL DB
dbPassword = dbutils.secrets.get(scope = databricksScopeName, key= passwordKey)
serverurl = 'jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServer, dbServerPortNumber,sqlDbName,dbUserName)
connectionProperties = {
    'password':dbPassword,
    'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}
df = spark.read.jdbc(url = serverurl, table = 'dbo.FileDetailsFormat', properties= connectionProperties)
display(df)

# COMMAND ----------

import pyspark.sql.functions as F

# Read file from landing
df1 = spark.read.csv(
    landingPath + fileName,
    inferSchema=True,
    header=True
)

display(df1)

errorFlag = False
errorMessage = ""

# Rule 1: Duplicate check
totalcount = df1.count()
distinctCount = df1.distinct().count()

print("Total count:", totalcount)
print("Distinct count:", distinctCount)

if distinctCount != totalcount:
    errorFlag = True
    errorMessage += "Duplication Found. Rule 1 Failed. "

# Rule 2: Date format validation
df2 = df.filter(df.FileName == fileNameWithoutExt).select(
    "ColumnName",
    "ColumnDateFormat"
)

metadataCount = df2.count()
print("Metadata rows found:", metadataCount)
display(df2)

# If no metadata rules found, reject file
if metadataCount == 0:
    errorFlag = True
    errorMessage += f"No metadata rules found for {fileNameWithoutExt}. "

rows = df2.collect()

for r in rows:
    colName = r["ColumnName"]
    colFormat = r["ColumnDateFormat"]

    print("Checking column:", colName, "Expected format:", colFormat)

    invalidCount = df1.filter(
        F.col(colName).isNotNull() &
        F.to_date(F.col(colName), colFormat).isNull()
    ).count()

    print("Invalid count for", colName, ":", invalidCount)

    if invalidCount > 0:
        errorFlag = True
        errorMessage += f"DateFormat is incorrect for {colName}. "
    else:
        print("All rows are good for", colName)

print("Final errorFlag:", errorFlag)
print("Final errorMessage:", errorMessage)

# Move file based on validation result
if errorFlag:
    dbutils.fs.mv(
        landingPath + fileName,
        rejectedPath + fileName
    )

    dbutils.notebook.exit(
        '{"errorFlag": "true", "errorMessage":"' + errorMessage + '"}'
    )

else:
    dbutils.fs.mv(
        landingPath + fileName,
        stagingPath + fileName
    )

    dbutils.notebook.exit(
        '{"errorFlag": "false", "errorMessage":"No error"}'
    )
# COMMAND ----------

# from pyspark.sql import functions as F

# # Read file from ADLS landing folder using direct ABFSS path
# df1 = spark.read.csv(
#     landingPath + fileName,
#     inferSchema=True,
#     header=True
# )

# # display(df1)

# # Rule validation
# errorFlag = False
# errorMessage = ""

# # Rule 1: Duplicate check
# totalcount = df1.count()
# print("Total count:", totalcount)

# distinctCount = df1.distinct().count()
# print("Distinct count:", distinctCount)

# if distinctCount != totalcount:
#     errorFlag = True
#     errorMessage = errorMessage + " Duplication Found. Rule 1 Failed "

# print(errorMessage)

# # Rule 2: Date format validation
# df2 = df.filter(
#     df.FileName == fileNameWithoutExt
# ).select(
#     "ColumnName",
#     "ColumnDateFormat"
# )

# rows = df2.collect()

# for r in rows:
#     colName = r[0]
#     colFormat = r[1]

#     print("Checking column:", colName, "Format:", colFormat)

#     invalidCount = df1.filter(
#         F.col(colName).isNotNull() &
#         F.to_date(F.col(colName), colFormat).isNull()
#     ).count()

#     print("Invalid count for", colName, ":", invalidCount)

#     if invalidCount > 0:
#         errorFlag = True
#         errorMessage = errorMessage + " DateFormat is incorrect for {} ".format(colName)
#     else:
#         print("All rows are good for", colName)

# print("Final error message:", errorMessage)

# # Move file based on validation result
# if errorFlag:
#     dbutils.fs.mv(
#         landingPath + fileName,
#         rejectedPath + fileName
#     )

#     dbutils.notebook.exit(
#         '{"errorFlag": "true", "errorMessage":"' + errorMessage + '"}'
#     )

# else:
#     dbutils.fs.mv(
#         landingPath + fileName,
#         stagingPath + fileName
#     )

#     dbutils.notebook.exit(
#         '{"errorFlag": "false", "errorMessage":"No error"}'
#     )