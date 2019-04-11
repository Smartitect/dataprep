
#%% [markdown]
# # Stage 4: Map column values
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
from commonCode import savePackage, openPackage, createFullPackagePath, openPackageFromFullPath, getTableStats, saveColumnInventoryForTable
from mappingCode import *

# Let's also set up global variables and common functions...
stageNumber = '4'
previousStageNumber = str(int(stageNumber) - 1)

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber + '_In.csv').to_pandas_dataframe()

#%%
# Output the inventory at this stage...
dataFiles

packageNameList = []
#%%
for index, row in dataFiles.iterrows():
    dataName = row["DataName"]
    config = row["Config"]
    packageName = row["PackageNameStage" + previousStageNumber]

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path {1}'.format(dataName, packageName))
    dataFlow = openPackageFromFullPath(packageName)

    transforms = load_transformation_configuration('./packages/' + dataName + '/' + dataName + '_Config.csv')

    if len(transforms) <= 1:
        continue;
    
    lookups = get_lookups_from_transforms(transforms)

    for key in lookups:
        lookupDictionary = lookups[key]
        replacements = []

        for lookup in lookupDictionary:
            replacements.append(ReplacementsValue(lookup, lookupDictionary[lookup]))

        destination_column = get_destination_column_name(key, transforms)
        dataFlow = dataFlow.map_column(key, destination_column, replacements)
        print(dataName + ': Transformed lookups for column - ' + key + '. Added new column ' + destination_column)

    fullPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)
