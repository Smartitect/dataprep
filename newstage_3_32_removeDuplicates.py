#%% [markdown]
# # Removing Duplicate Rows
# To apply a genric approach to filtering out duplicate rows in tables:
# - We will use simple **distinct** function initially
# - It would be good to tihnk of means we can use to quarantine the duplicates without breaking out into pandas dataframe!

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonDataFlowProcessingLoop import dataFlowProcessingLoop
from commonInventoryCreation import getColumnStats, getDataFlowStats
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage

# Let's also set up global variables and common functions...
previousStageNumber = '31'
thisStageNumber = '32'

#%%
def removeDuplicates(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        if operationFlag != '':

            columnsToKeep = operationFlag

            numberOfRowsBefore = dataFlow.row_count

            dataFlow = dataFlow.distinct(dprep.ColumnSelector(columnsToKeep, True, True, invert=False))
            print('{0}: removed duplicates from column {1} rows before {2} rows afer {3}'.format(dataName, operationFlag, numberOfRowsBefore, dataFlow.row_count))
        
        else:
            print('{0}: no duplicate processing required'.format(dataName))

        dataProfile = dataFlow.get_profile()

        # Now generate column and data flow inventories
        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventory = getDataFlowStats(dataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(dataFlow, dataName, thisStageNumber, qualityFlag)
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))

        return dataFlow, columnInventory, dataFlowInventory

    else:
        print('{0}: no package file found at location {1}'.format(dataName, fullPackagePath))
        return None, None, None

#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'RemoveDuplicates', removeDuplicates)

#%%
dataFlowInventoryAll