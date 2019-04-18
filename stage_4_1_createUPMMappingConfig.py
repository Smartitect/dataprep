
#%% [markdown]
# # Stage: Map column values
# Purpose of this stage 

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import value, ReplacementsValue
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, openPackageFromFullPath, getTableStats, saveColumnInventoryForTable, saveDataFileInventory, gatherEndStageStats
from mappingCode import createUPMMappingConfigFromDataFlow

# Let's also set up global variables and common functions...
stageNumber = '41'
previousStageNumber = '40'
nextStageNumber = '50'
packagePath = "./packages"

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber +'_In.csv').to_pandas_dataframe()

#%%
# Output the inventory at this stage...
dataFiles

packageNameList = []
configFileList = []
config = pd.DataFrame()

#%%
for index, row in dataFiles.iterrows():
    dataName = row["DataName"]
    packageName = row["PackageNameStage" + previousStageNumber]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageName))
    dataFlow = openPackageFromFullPath(packageName)

    fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)
    
    configPath = createUPMMappingConfigFromDataFlow(dataFlow, dataName)
    configFileList.append(configPath)


#%%
# Capture the stats
dataFiles = gatherEndStageStats(stageNumber, dataFiles, [], [], packageNameList)

configFilesCol = pd.DataFrame({'MappingConfig':configFileList})
dataFiles = pd.concat([dataFiles, configFilesCol], axis=1)

#%%
# Write the inventory out for the next stage in the process to pick up
saveDataFileInventory(dataFiles, stageNumber, nextStageNumber)