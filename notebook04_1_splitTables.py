#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import seaborn as sns
import pandas_profiling
import datetime
import shutil
from azureml.dataprep import value, ReplacementsValue
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, getTableStats, saveColumnInventoryForTable

#%%
sourceFileName = 'USERDATA'
lookupFileName = 'LOOKUPS'
stageNumber = '3'
previousStageNumber = str(int(stageNumber) - 1)
packagePath = './packages/'

#%%
dataFiles = dprep.read_csv('dataFileInventory.csv').to_pandas_dataframe()
dataFlow = Dataflow.open(packagePath + sourceFileName + '/' + previousStageNumber + '/' + sourceFileName +'_A_package.dprep')
lookupsDataFlow = Dataflow.open(packagePath + lookupFileName + '/' + previousStageNumber + '/' + lookupFileName + '_A_package.dprep')
columnInventoryAllTables = dprep.read_csv('columnInventory_' + previousStageNumber + '_Out.csv').to_pandas_dataframe()

#%%
dataFlow = dataFlow.map_column('FORM', 'FORM2', {ReplacementsValue('EXITDEFERRED', 'EXITDEFERREDS'), ReplacementsValue('EXITRETIRED', 'EXITRETIREMENT'), ReplacementsValue('EXITDEFERRED', 'EXITDEFERREDS')})
dataFlow = dataFlow.drop_columns('FORM')
dataFlow = dataFlow.rename_columns({'FORM2': 'FORM'})

#%%
dataProfile = dataFlow.get_profile()
rowCount = dataFlow.row_count

newRowCounts = []
#%%
for col in dataProfile.columns['FORM'].value_counts:
    newDataFlow = dataFlow.filter(dataFlow['FORM'] == col.value)

    filteredLookupsDataFlow = lookupsDataFlow.filter((lookupsDataFlow['LOOKTYPE'] == 'U') & (lookupsDataFlow['LFIELD'] == col.value))
    filteredLookupsDataFlow.head(100)
    
    lookupsDataFrame = filteredLookupsDataFlow.to_pandas_dataframe()

    replacedColumns = []

    for index, row in lookupsDataFrame.iterrows():
        partitionOccurred = True
        originalValue = row['COMPNAME']
        replacementValue = row['LDESC']

        replacedColumns.append(replacementValue)
        newDataFlow = newDataFlow.rename_columns({originalValue: replacementValue})

    newDataProfile = newDataFlow.get_profile()

    for column in newDataProfile.columns.values():
        if column.min is None and column.max is None:
            newDataFlow = newDataFlow.drop_columns(column.name)

    newDataFlow.head(5)

    if partitionOccurred:
        packageName = sourceFileName + '_' + col.value

        if os.path.isdir(packagePath + packageName):
            shutil.rmtree(packagePath + packageName)

        os.mkdir(packagePath + packageName)

        savePackage(newDataFlow, packageName, stageNumber, 'A')

        # Profile the table
        newDataProfile = newDataFlow.get_profile()
        newRowCounts.append(newDataFlow.row_count)

        columnInventory = getTableStats(newDataProfile, packageName, stageNumber)
        saveColumnInventoryForTable(columnInventory, packageName, stageNumber)
        columnInventoryAllTables = columnInventoryAllTables.append(columnInventory)
    
#%%
dataFiles.to_csv('dataFileInventory.csv', index = None)

columnInventoryAllTables.to_csv('columnInventory_' + stageNumber + '_Out.csv', index = None)


