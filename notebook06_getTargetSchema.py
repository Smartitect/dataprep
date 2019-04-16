#%% [markdown]
# # Stage 6 : Get Target Schema
# Haven't figured out how best to do this yet!
# It would be great if you could define some canonical form, or create from an existing file / database table schema?
# Then you could apply some ML to infer / learn how to map?

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import pyodbc
import urllib
import sqlalchemy
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath


#%%
sourceFileName = 'PEOPLE'
previousStageNumber = '2'
stageNumber = '3'

#%%
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber + '_In.csv').to_pandas_dataframe()
dataFlow = Dataflow.open('./packages/' + sourceFileName + '/' + previousStageNumber + '/' + sourceFileName +'_A_package.dprep')

#%%
# Set SQL connection string and SQL command string...
server = 'glatesttpasql01'
database = 'HRDev_POC'
driver = 'DRIVER={SQL Server Native Client 11.0};SERVER='+ server +';DATABASE='+ database +';Trusted_Connection=yes;Connection Timeout=120'
conn = pyodbc.connect(driver)
cursor = conn.cursor()

#%%
# Get target schema
strSQL = 'SELECT OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) AS [Schema], T.[name] AS [table_name], AC.[name] AS [column_name], TY.[name] AS system_data_type, AC.[max_length], AC.[precision], AC.[scale], AC.[is_nullable], AC.[is_ansi_padded] FROM sys.[tables] AS T INNER JOIN sys.[all_columns] AC ON T.[object_id] = AC.[object_id] INNER JOIN sys.[types] TY ON AC.[system_type_id] = TY.[system_type_id] AND AC.[user_type_id] = TY.[user_type_id] WHERE T.[is_ms_shipped] = 0 ORDER BY T.[name], AC.[column_id];'
TargetSchema = pd.read_sql(strSQL, conn)

#%%
# Get target table
TableName = 'UPMPERSON'
strSQL = 'SELECT TOP 0 * FROM ' + TableName + ';'
TargetTable = pd.read_sql(strSQL, conn)

#%%
# NOTE Temp cell to force deletion of UPMPERSON primary key column 
TargetTable = TargetTable.drop('PERSONID',axis=1)

#%%
# NOTE Move out to a config table
# Define data type mapping
def fix_datatype (row):
    if row['system_data_type'] == 'int' or row['system_data_type'] == 'bigint' or row['system_data_type'] == 'bit':
        return 'int64'
    if row['system_data_type'] == 'datetime' or row['system_data_type'] == 'smalldatetime':
        return 'datetime64[ns]'
    if row['system_data_type'] == 'numeric' or row['system_data_type'] == 'decimal' or row['system_data_type'] == 'float':    
        return 'float64'
    return 'object'   

#%%
# Add new column to TargetSchema with pandas dtype equivalents
TargetSchema['pd_datatype'] = TargetSchema.apply (lambda row: fix_datatype(row), axis=1)

#%%
# Add new column to TargetScheme with a concatenated string of table name and column name to uniquely identify columns 
TargetSchema['Unique_ID'] = TargetSchema[['table_name', 'column_name']].apply(lambda x: '_'.join(x), axis=1)

#%%
# Get dtypes of TargetSchema and change TargetTable dtypes 
for column in TargetTable.columns[:]:
    #print(column)
    #print(TargetTable[column].dtype) 
    lookup_value = TableName + "_" + TargetTable[column].name
    lookup_index = TargetSchema.index[TargetSchema['Unique_ID'] == lookup_value].tolist()
    lookup_df = TargetSchema.loc[lookup_index, 'pd_datatype']
    targ_dtype = lookup_df.iloc[0][:]
    #print(targ_dtype)
    TargetTable[column].astype(targ_dtype)


#%%
dataFlow = dataFlow.drop_columns(dprep.ColumnSelector('NINO|DOB|FORENAME', True, True, invert=True))
dataFlow = dataFlow.rename_columns({'FORENAME':'FORENAMES'})

dataFlow.head(5)

#%%
dataFrame = dataFlow.to_pandas_dataframe()
TargetTable = TargetTable.append(dataFrame)

#%%
#cursor.execute('ALTER TABLE UPMPERSON DISABLE TRIGGER ALL')
params = urllib.parse.quote_plus(driver)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
TargetTable.to_sql('UPMPERSON',engine,'dbo','append',False)
#cursor.execute('ALTER TABLE UPMPERSON ENABLE TRIGGER ALL')

