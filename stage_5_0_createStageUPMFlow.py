
#%% [markdown]
# # Stage: Create Stage UPM Flow
# Purpose of this stage 

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import shutil
from azureml.dataprep import value, ReplacementsValue
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, openPackageFromFullPath, getTableStats, saveColumnInventoryForTable, saveDataFileInventory, gatherEndStageStats, createNewPackageDirectory

# Let's also set up global variables and common functions...
stageNumber = '50'
previousStageNumber = '41'
nextStageNumber = '60'

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber +'_In.csv').to_pandas_dataframe()

#%%
# Output the inventory at this stage...
dataFiles

packageNameList = []
#%%

for index, row in dataFiles.iterrows():
    dataName = row["DataName"]
    mappingConfigPath = row["MappingConfig"]
    mappingConfig = dprep.read_csv(mappingConfigPath).to_pandas_dataframe()

    packageName = row["PackageNameStage" + previousStageNumber]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageName))
    dataFlow = openPackageFromFullPath(packageName)

    targetDataFlow = dataFlow
    columnsToKeep = ''

    for sourceTable in mappingConfig[mappingConfig.SourceTable == dataName]['SourceTable'].unique():
        for sourceColumn, targetColumn in mappingConfig[mappingConfig.SourceTable == sourceTable][['SourceColumn', 'TargetColumn']].values:
            if columnsToKeep is '':
                columnsToKeep = targetColumn
            else:                
                columnsToKeep = columnsToKeep + '|' + targetColumn
            
            targetDataFlow = targetDataFlow.rename_columns({sourceColumn: targetColumn})

    targetDataFlow = targetDataFlow.drop_columns(dprep.ColumnSelector(columnsToKeep, True, True, invert=True))
    newPackageName = next(iter(mappingConfig[mappingConfig.SourceTable == dataName]['TargetTable'].unique()))

    createNewPackageDirectory(newPackageName)
    fullPackagePath = savePackage(targetDataFlow, newPackageName, stageNumber, 'A')

    # Profile the table
    newDataProfile = targetDataFlow.get_profile()

    columnInventory = getTableStats(newDataProfile, newPackageName, stageNumber)
    saveColumnInventoryForTable(columnInventory, newPackageName, stageNumber)
    dataFiles = dataFiles.append({ 'DataName' : newPackageName, 'Archived' : False, 'PackageNameStage'+previousStageNumber: fullPackagePath}, ignore_index=True)

for index, row in dataFiles.iterrows():
    dataName = row["DataName"]
    config = row["Config"]
    packageName = row["PackageNameStage" + previousStageNumber]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageName))
    dataFlow = openPackageFromFullPath(packageName)

    fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)

#%%
# Capture the stats
dataFiles = gatherEndStageStats(stageNumber, dataFiles, [], [], packageNameList)

#%%
# Write the inventory out for the next stage in the process to pick up
saveDataFileInventory(dataFiles, stageNumber, nextStageNumber)