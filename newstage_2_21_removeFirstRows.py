
#%% [markdown]
# # Stage : Remove First Rows
# This involves stepping through each file in the confiug file to extract and do a basic clean up:
# - Based on flag in config, remove the first row;
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
from commonPackageHandling import saveDataFlowPackage, openDataFlowPackage
from commonInventoryCreation import getColumnStats, getDataFlowStats

# Let's also set up global variables and common functions...
previousStageNumber = '20'
thisStageNumber = '21'

#%%
def removeRowsFromTop(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):
    
    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        # Now perform the operation on the dataFlow : ie remove the number of rows specified from the top
        numberOfRowsToRemove = int(operationFlag)
        dataFlow = dataFlow.skip(numberOfRowsToRemove)
        print('{0}: removed first {1} row(s)'.format(dataName, numberOfRowsToRemove))
        
        dataProfile = dataFlow.get_profile()

        # Now generate column and data flow inventories
        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventory = getDataFlowStats(dataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(dataFlow, dataName, thisStageNumber, qualityFlag)
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))

        # Now return all of the components badk to the main loop...
        return dataFlow, columnInventory, dataFlowInventory

    else:
        print('{0}: no package file found at location {1}'.format(dataName, fullPackagePath))
        return None, None, None


#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'RemoveRowsFromTop', removeRowsFromTop)

#%%
dataFlowInventoryAll

