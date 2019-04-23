
#%% [markdown]
# # Stage 2 - Ingest All Files
# The purpose of this stage is to load all data successfully into "data flows" ready for downstream processing.
# We we will focus on loading all of the data in its raw form.  Therefore - we will perform no transforms or clean up steps - that will come later!
# We will gather high level statistics such as column and row counts by stepping through each file in the inventory:
# - Read the first header row of each file and determine exepcted number of columns;
# - Load the CSV data;
# - Count the actual number of columns;
# - Count the actual number of rows;
# - Save the data flow that has been created for each file away so that it can be referenced and used later on.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import pandas_profiling as pp
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonInventoryCreation import getColumnStats, getDataFlowStats, saveColumnInventory, saveDataFlowInventory
from commonPackageHandling import saveDataFlowPackage

# Let's also set up global variables...
previousStageNumber = '00'
thisStageNumber = '10'

#%%
# Load in file names to be processed from the data file inventory
dataFileStats = dprep.read_csv('dataFileInventory_' + previousStageNumber +'.csv').to_pandas_dataframe()

#%%
# First a quick pass through each file to grab the number of headers and count columns
# NOTE - this loop could improved such that there is less code the dataFileStats dataframe above
headerCount = []
for index, row in dataFileStats.iterrows():
    firstRow = open(row["FullFilePath"]).readline().strip()
    regexPattern = re.compile(',\w')
    patternCount = len(re.findall(regexPattern,firstRow))
    headerCount.append(patternCount + 1)
    print(firstRow)
    print(patternCount)
headerCountCol = pd.DataFrame({'HeaderCount':headerCount})
dataFileStats = pd.concat([dataFileStats, headerCountCol], axis=1)

#%%
# Save the file inventory away with the addition of the header count
dataFileStats.to_csv('dataFileInventory_' + thisStageNumber + '.csv', index = None)

#%%
# Can't call the dataFlowProcessingLoop function this time around.  Will need to write something more specialised...
# Load the dataFlow controller file
dataFlows = dprep.read_csv('dataFlowController.csv').to_pandas_dataframe()

# Set up empty dataframes that we will use to build up inventories at both dataFlow and column level
dataFlowInventoryAll = pd.DataFrame()
columnInventoryAll = pd.DataFrame()

for index, row in dataFlows.iterrows():

    dataName = row["DataName"]
    operatorToUse = "IngestFile"
    operationFlag = row[operatorToUse]
    fullFilePath = row["FullFilePath"]

    if operationFlag == 'Yes':
        print('{0}: loading data from file path {1}'.format(dataName, fullFilePath))
        newDataFlow = dprep.read_csv(fullFilePath)

        dataProfile = newDataFlow.get_profile()

        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        columnInventoryAll = columnInventoryAll.append(columnInventory)
        print('{0}: generated column inventory'.format(dataName))

        dataFlowInventory = getDataFlowStats(newDataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventoryAll = dataFlowInventoryAll.append(dataFlowInventory)
        print('{0}: generated data flow inventory'.format(dataName))

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(newDataFlow, dataName, thisStageNumber, 'A')
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))
    else:
        print('{0}: no package file created.'.format(dataName))

    # Once we have processed all dataflows, we save the inventories away
    saveColumnInventory(columnInventoryAll, thisStageNumber)
    saveDataFlowInventory(dataFlowInventoryAll, thisStageNumber)

#%%
dataFlowInventoryAll

#%%
columnInventoryAll
