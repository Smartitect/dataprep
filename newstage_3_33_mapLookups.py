#%% [markdown]
# Map values to lookup dictionary
# Uses a config file to drive mappings and looks at a mapping file to get the values to replace

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import col, ReplacementsValue
from azureml.dataprep import Dataflow
from commonDataFlowProcessingLoop import dataFlowProcessingLoop
from commonInventoryCreation import getColumnStats, getDataFlowStats
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage
from mappingCode import load_transformation_configuration, get_lookups_from_transforms, get_destination_column_name

# Let's also set up global variables and common functions...
previousStageNumber = '32'
thisStageNumber = '33'

#%%
def mapLookups(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        if operationFlag != '':

            transforms = load_transformation_configuration('./Config/' + operationFlag)

            if len(transforms) > 1:
                lookups = get_lookups_from_transforms(transforms)

                for key in lookups:
                    lookupDictionary = lookups[key]
                    replacements = []

                    dataFlow = dataFlow.set_column_types({key: dprep.FieldType.STRING})

                    for lookup in lookupDictionary:
                        
                       replacements.append(ReplacementsValue(lookup, lookupDictionary[lookup]))

                    destination_column = get_destination_column_name(key, transforms)
                    dataFlow = dataFlow.map_column(key, destination_column, replacements)
                    print(dataName + ': Transformed lookups for column - ' + key + '. Added new column ' + destination_column)    
        
        else:
            print('{0}: no look-up processing required'.format(dataName))

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
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'MapLookups', mapLookups)

#%%
dataFlowInventoryAll