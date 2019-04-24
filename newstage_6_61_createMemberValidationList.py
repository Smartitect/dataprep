#%% [markdown]
# # Stage : Create a list of members to run validations against
# Requires UPMFOLDER_UPMPERSON join to have occurred and data populated in the table
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
previousStageNumber = '60'
thisStageNumber = '61'

def createMemberValidationList(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):

    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        columnInventoryIntermediate = pd.DataFrame()

        if operationFlag != '':
       
            targetDataFlow = dataFlow
            columnsToKeep = 'FOLDERID|NINO|UPPERSURNAME|FORENAMES|FOLDERSTATUSFG'

            targetDataFlow = targetDataFlow.drop_columns(dprep.ColumnSelector(columnsToKeep, True, True, invert=True))
            newPackageName = 'MemberValidationList'

            createNewPackageDirectory(newPackageName)
            saveDataFlowPackage(targetDataFlow, newPackageName, thisStageNumber, 'A')

            profile = targetDataFlow.get_profile()

            columnInventory = getColumnStats(profile, newPackageName, thisStageNumber, operatorToUse, operationFlag)
            columnInventoryIntermediate = columnInventoryIntermediate.append(columnInventory)

        else:
            print('{0}: no member list required'.format(dataName))

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
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'CreateMemberList', createMemberValidationList)

#%%
dataFlowInventoryAll