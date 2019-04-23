
#%% [markdown]
# # Stage : Parse Nulls
# - Based on flag in config, replace the custom string `<null>` representing a null and any other empty cells to a real `null`;
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

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
previousStageNumber = '10'
thisStageNumber = '20'

#%%
def parseNulls(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        if operationFlag != '':
            # Get a list of the columns and count them...
            dataFlowColumns = list(dataFlow.get_profile().columns.keys())
            # Replace any occurences null, using custom, across all columns...
            dataFlow = dataFlow.replace_na(dataFlowColumns, custom_na_list=operationFlag)
            print('{0}: parsed nulls including custom string {1} from {2} columns'.format(dataName, operationFlag, len(dataFlowColumns)))
        
        dataProfile = dataFlow.get_profile()

        # Now generate column and data flow inventories
        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventory = getDataFlowStats(dataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(dataFlow, dataName, thisStageNumber, 'A')
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))

        return dataFlow, columnInventory, dataFlowInventory

    else:
        print('{0}: no package file found at location {1}'.format(dataName, fullPackagePath))
        return None, None, None

#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'ParseNullString', parseNulls)

#%%
dataFlowInventoryAll
