
#%% [markdown]
# # Stage : Automatically Detect Types
# This involves stepping through each file in the confiug file to extract and do a basic clean up:
# - Try to detect data types in each column using **column types builder**
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, openPackageFromFullPath, getTableStats, saveColumnInventoryForTable, saveDataFileInventory, gatherStartStageStats, gatherEndStageStats
from mappingCode import createConfigFromDataFlow, createDummyConfigFromDataFlow

# Let's also set up global variables and common functions...
stageNumber = '23'
previousStageNumber = str(int(stageNumber) - 1)
nextStageNumber = '30'

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber +'_In.csv').to_pandas_dataframe()

#%%
# Output the inventory at this stage...
dataFiles

#%%
columnCountStartList = []
columnCountEndList = []
rowCountStartList = []
rowCountEndList = []
packageNameList = []
dataInventoryAllTables = pd.DataFrame()
for index, row in dataFiles.iterrows():

    dataName = row["DataName"]
    packageNameStage01 = row["PackageNameStage" + previousStageNumber]
    headerCount = int(row["HeaderCount"])

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageNameStage01))
    dataFlow = openPackageFromFullPath(packageNameStage01)

    # Start: Count the rows...
    rowCountStart = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCountStart))
    rowCountStartList.append(rowCountStart)
  
    # Get a list of the columns and count them...
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    print('{0}: found {1} columns, expected {2}'.format(dataName, len(dataFlowColumns), headerCount))

    columnCountStart = len(dataFlowColumns)
    columnCountStartList.append(columnCountStart)

    # Detect and apply column types
    builder = dataFlow.builders.set_column_types()
    builder.learn()
    builder.ambiguous_date_conversions_keep_month_day()
    dataFlow = builder.to_dataflow()

    # Get a list of the columns and count them...
    dataFlowColumnsEnd = list(dataFlow.get_profile().columns.keys())
    columnCountEnd = len(dataFlowColumnsEnd)
    columnCountEndList.append(columnCountEnd)

    # End: Count the rows...
    rowCountEnd = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCountEnd))
    rowCountEndList.append(rowCountEnd)

    # Capture number of columns found...
    print('{0}: at end there are now {1} columns, expected {2}'.format(dataName, columnCountEnd, headerCount))

    # Profile the table
    dataProfile = dataFlow.get_profile()
    dataInventory = getTableStats(dataProfile, dataName, stageNumber)
    # NOTE - should put extra statements in here to export dataInventory to Lian's new folder structure
    saveColumnInventoryForTable(dataInventory, dataName, stageNumber)
    
    dataInventoryAllTables = dataInventoryAllTables.append(dataInventory)

    # Finally save the data flow so it can be used later
    fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)

#%%
# Capture the stats
dataFiles = gatherStartStageStats(stageNumber, dataFiles, rowCountStartList, columnCountStartList)
dataFiles = gatherEndStageStats(stageNumber, dataFiles, rowCountEndList, columnCountEndList, packageNameList)

#%%
# Write the inventory out for the next stage in the process to pick up
saveDataFileInventory(dataFiles, stageNumber, nextStageNumber)

#%%
dataInventoryAllTables.to_csv('columnInventory_' + stageNumber + '_Out.csv', index = None)
