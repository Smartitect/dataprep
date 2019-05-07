
#%% [markdown]
# # Stage : Clean Up Address
# There is a need to replace the CR/LF `\0d0a` string with a comma in the address, and replace empty values with an empty string.
# Will also attempt to split address into component parts are separate columns

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
previousStageNumber = '33'
thisStageNumber = '34'

#%%
def cleanUpAddress(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        if operationFlag != '':
            
            # First replace the escape character with comma + space
            dataFlow = dataFlow.str_replace(operationFlag, r'\0d0a', ', ')
            
            # Now capture any empty addresses
            dataFlow = dataFlow.replace(operationFlag, r', , , ', None)
            print('{0}: cleaned up addresses in column {1}'.format(dataName, operationFlag))

            # Will now try to split out the address into component columns...
            # builder = dataFlow.builders.split_column_by_example(source_column=operationFlag)
            # builder.add_example(example=('552 Malvern Avenue, St. George, Bristol', \
            #    ['552 Malvern Avenue', 'St. George', 'Bristol']))
            # builder.add_example(example=('224A Tomswood St, Hainault, Ilford, Cornwall', \
            #     ['224A Tomswood St', 'Hainault', 'Ilford', 'Cornwall']))
            # dataFlow = builder.to_dataflow()

            # dataFlow = dataFlow.split_column_by_delimiters(operationFlag, ', ', True)


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
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'CleanUpAddress', cleanUpAddress)

#%%
dataFlowInventoryAll
