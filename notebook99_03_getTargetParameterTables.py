#%% [markdown]
# # Stage 99_03 : Get Target Parameter Tables
# # These Tables will be required to populate the import tables with static values and column lookup codes
# NOTE - The data selection is driven by a config file that could ultimately be held as a SQL Server table or as blob. 

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import pyodbc
#import urllib
#import sqlalchemy
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath

#%%
# Import config file with list of UPM Tables to be queried via SQL...
# NOTE - ultimately this could point to other storage sources such as blob on Azure
configPath = './config/Get_Parameter_Tables_CONFIG.csv'
configFile = pd.read_csv(configPath)

#%%
# Set SQL connection string and SQL command string...
server = 'glatesttpasql01'
database = 'HRDev_POC'
driver = 'DRIVER={SQL Server Native Client 11.0};SERVER='+ server +';DATABASE='+ database +';Trusted_Connection=yes;Connection Timeout=120'
conn = pyodbc.connect(driver)

#%%
# Loop through each row of the Config File and create Tables based upon SQL select statements
for row in configFile.itertuples():
    TableName = row.table_name
    strSQL = 'SELECT * FROM ' + TableName + ';'
    TargetTable = pd.read_sql(strSQL, conn)
    TargetTable.to_csv('./TargetParameterTables/'+TableName+'.csv')



