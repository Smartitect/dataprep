
#%% [markdown]
# # Stage : Quarantine Rows
# This involves stepping through each file in the confiug file to extract and do a basic clean up:
# - Quarantine rows (extract them and put them into a parallel data flow so that they can be fixed at a later stage) which have values in columns that are not listed in the header record;
# - Drop any columns that we weren't expecting
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
stageNumber = '22'
previousStageNumber = str(int(stageNumber) - 1)
nextStageNumber = str(int(stageNumber) + 1)

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
quarantinedRowsList = []
packageNameList = []
dataInventoryAllTables = pd.DataFrame()
for index, row in dataFiles.iterrows():

    dataName = row["DataName"]
    packageNameStage01 = row["PackageNameStage" + previousStageNumber]
    headerCount = int(row["HeaderCount"])
    removeFirstRow = row["RemoveFirstRow"]
    parseNullString = row["ParseNullString"]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageNameStage01))
    dataFlow = openPackageFromFullPath(packageNameStage01)

    # Count the rows...
    rowCountStart = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCountStart))
    rowCountStartList.append(rowCountStart)
  
    # Get a list of the columns and count them...
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    print('{0}: found {1} columns, expected {2}'.format(dataName, len(dataFlowColumns), headerCount))

    columnCountStart = len(dataFlowColumns)
    columnCountStartList.append(columnCountStart)

    # Quarantine rows which don't have values in the extra columns
    if headerCount != len(dataFlowColumns):
        # NOTE - this logic assumes that all unwanted columns are on the far right, this could be improved!
        # Fork a new data flow with rows that have data in the un-expected columns
        quarantinedDataFlow = dataFlow.drop_nulls(dataFlowColumns[headerCount:])
        if quarantinedDataFlow.row_count is None:
            quarantinedRowCount = 0 
        else:
            quarantinedRowCount = quarantinedDataFlow.row_count
            
        
        print('{0}: created quarantined data with {1} rows'.format(dataName, quarantinedRowCount))
        quarantinedRowsList.append(quarantinedRowCount)
        # Finally save the data flow so it can be used later
        fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'B')
        print('{0}: saved quarantined data to {1}'.format(dataName, fullPackagePath))
    else:
        quarantinedRowsList.append(0)

    # Filter out the quarantined rows from the main data set
    # NOTE : can't figure out a better way of doign this for now - see note below...
    for columnToCheck in dataFlowColumns[headerCount:]:
        # NOTE - don't know why commented line of code below doesn't work!
        # dataFlow = dataFlow.filter(dataFlow[columnToCheck] != '')
        dataFlow = dataFlow.assert_value(columnToCheck, value != '' , error_code='ShouldBeNone')
        dataFlow = dataFlow.filter(col(columnToCheck).is_error())
        print('{0}: filtered column {1}, row count now {2}'.format(dataName, columnToCheck, dataFlow.row_count))
    
    # Count the rows...
    rowCountEnd = dataFlow.row_count
    rowCountEndList.append(rowCountEnd)

    # Now drop the extra columns
    dataFlow = dataFlow.drop_columns(dataFlowColumns[headerCount:])
    print('{0}: dropped {1} unwanted columns'.format(dataName, len(dataFlowColumns[headerCount:])))
    
    # Get a list of the columns and count them...
    dataFlowColumnsEnd = list(dataFlow.get_profile().columns.keys())
    columnCountEnd = len(dataFlowColumnsEnd)
    columnCountEndList.append(columnCountEnd)

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

quarantinedRowsCol = pd.DataFrame({'QuarantinedRowsStage' + stageNumber:quarantinedRowsList})
dataFiles = pd.concat([dataFiles, quarantinedRowsCol], axis=1)

dataFiles = gatherEndStageStats(stageNumber, dataFiles, rowCountEndList, columnCountEndList, packageNameList)

#%%
# Write the inventory out for the next stage in the process to pick up
saveDataFileInventory(dataFiles, stageNumber, nextStageNumber)

#%%
dataInventoryAllTables.to_csv('columnInventory_' + stageNumber + '_Out.csv', index = None)