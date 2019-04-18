#%% [markdown]
# # Stage: Split Tables
# Purpose of this stage 

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
from commonCode import savePackage, openPackage, openPackageFromFullPath, createFullPackagePath, getTableStats, saveColumnInventoryForTable, saveDataFileInventory, gatherStartStageStats, gatherEndStageStats, createNewPackageDirectory
from mappingCode import createConfigFromDataFlow, createDummyConfigFromDataFlow

#%%
sourceFileName = 'USERDATA'
lookupFileName = 'LOOKUPS'
stageNumber = '30'
previousStageNumber = '23'
nextStageNumber = '40'
packagePath = './packages/'

#%%
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber +'_In.csv').to_pandas_dataframe()
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

        createNewPackageDirectory(packageName)
        packagePath = savePackage(newDataFlow, packageName, stageNumber, 'A')

        # Profile the table
        newDataProfile = newDataFlow.get_profile()
        newRowCounts.append(newDataFlow.row_count)

        columnInventory = getTableStats(newDataProfile, packageName, stageNumber)
        saveColumnInventoryForTable(columnInventory, packageName, stageNumber)
        #FileName,FullFilePath,FileSize,Modified,DataName,Archived,HeaderCount,ColumnCountStage1,RowCountStartStage1,PackageNameStage1,RemoveFirstRow,ParseNullString,RowCountStartStage2,RowCountEndStage2,QuarantinedRowsStage2,ColumnCountEndStage2,PackageNameStage2,Config
        dataFiles = dataFiles.append({ 'DataName' : packageName, 'Archived' : False, 'PackageNameStage'+previousStageNumber: packagePath}, ignore_index=True)

#%%
columnCountStartList = []
columnCountEndList = []
rowCountStartList = []
rowCountEndList = []
packageNameList = []
configFileList = []
dataInventoryAllTables = pd.DataFrame()
for index, row in dataFiles.iterrows():
    
    dataName = row["DataName"]
    packageNameStage01 = row["PackageNameStage" + previousStageNumber]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageNameStage01))
    dataFlow = openPackageFromFullPath(packageNameStage01)

    # Count the rows...
    rowCountStart = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCountStart))
    rowCountStartList.append(rowCountStart)

    # Get a list of the columns and count them...
    dataFlowColumnsStart = list(dataFlow.get_profile().columns.keys())
    columnCountStart = len(dataFlowColumnsStart)
    columnCountStartList.append(columnCountStart)
   
    # Get a list of the columns and count them...
    dataFlowColumnsEnd = list(dataFlow.get_profile().columns.keys())
    columnCountEnd = len(dataFlowColumnsEnd)
    columnCountEndList.append(columnCountEnd)

    rowCountEnd = dataFlow.row_count
    rowCountEndList.append(rowCountEnd)

    # Create config file for table
    configPath = createDummyConfigFromDataFlow(dataFlow, dataName)
    configFileList.append(configPath)

    # Profile the table
    dataProfile = dataFlow.get_profile()
    dataInventory = getTableStats(dataProfile, dataName, stageNumber)
    
    dataInventoryAllTables = dataInventoryAllTables.append(dataInventory)

    # Finally save the data flow so it can be used later
    fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)

#%%
# Capture the stats
dataFiles = gatherStartStageStats(stageNumber, dataFiles, rowCountStartList, columnCountStartList)
dataFiles = gatherEndStageStats(stageNumber, dataFiles, rowCountEndList, columnCountEndList, packageNameList)

configFilesCol = pd.DataFrame({'Config':configFileList})
dataFiles = pd.concat([dataFiles, configFilesCol], axis=1)

#%%
# Write the inventory out for the next stage in the process to pick up
saveDataFileInventory(dataFiles, stageNumber, nextStageNumber)

dataInventoryAllTables.to_csv('columnInventory_' + stageNumber + '_Out.csv', index = None)


