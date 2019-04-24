#%% [markdown]
# # Stage : Create UPM dataflows
# Uses a config file to drive which UPM dataflows we should create
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
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage, createNewPackageDirectory
from mappingCode import load_transformation_configuration, get_lookups_from_transforms, get_destination_column_name

# Let's also set up global variables and common functions...
previousStageNumber = '41'
thisStageNumber = '50'

#%%
def createUPMDataflow(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        columnInventoryIntermediate = pd.DataFrame()
        if operationFlag != '':

            mappingConfig = dprep.read_csv('./Config/' + operationFlag).to_pandas_dataframe()

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
            saveDataFlowPackage(targetDataFlow, newPackageName, thisStageNumber, 'A')

            profile = targetDataFlow.get_profile()

            columnInventory = getColumnStats(profile, newPackageName, thisStageNumber, operatorToUse, operationFlag)
            columnInventoryIntermediate = columnInventoryIntermediate.append(columnInventory)

        else:
            print('{0}: no duplicate processing required'.format(dataName))

        dataProfile = dataFlow.get_profile()

        # Now generate column and data flow inventories
        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventory = getDataFlowStats(dataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)

        columnInventoryIntermediate.append(columnInventory)

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(dataFlow, dataName, thisStageNumber, qualityFlag)
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))

        return dataFlow, columnInventoryIntermediate, dataFlowInventory

    else:
        print('{0}: no package file found at location {1}'.format(dataName, fullPackagePath))
        return None, None, None

#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'UPMDataFlowMapping', createUPMDataflow)

#%%
dataFlowInventoryAll