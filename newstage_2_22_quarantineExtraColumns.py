
#%% [markdown]
# # Stage : Quarantine Rows
# This involves stepping through each file in the confiug file to extract and do a basic clean up:
# - Quarantine rows (extract them and put them into a parallel data flow so that they can be fixed at a later stage) which have values in columns that are not listed in the header record;
# - Drop any columns that we weren't expecting
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import col
from azureml.dataprep import value
from azureml.dataprep import Dataflow
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage
from commonDataFlowProcessingLoop import dataFlowProcessingLoop
from commonInventoryCreation import getColumnStats, getDataFlowStats

# Let's also set up global variables and common functions...
previousStageNumber = '21'
thisStageNumber = '22'

def quarantineRows(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):
    
    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        # Now perform the operation on the dataFlow : ie remove the number of rows specified from the top
        
        # First count the number of columns found 
        dataFlowColumns = list(dataFlow.get_profile().columns.keys())
        numberOfColumnsFound = len(dataFlowColumns)

        # Now convert the operationFlag to an integer
        headerCount = int(operationFlag)

        # If we have more columns that expected, we quarantine rows which have values in the extra columns
        if numberOfColumnsFound > headerCount:
            # NOTE - this logic assumes that all unwanted columns are on the far right, this could be improved!
            # Fork a new data flow with rows that have data in the un-expected columns
            print('{0}: we have found {1} columns, expected {2} so will now quarantine any rows with data in them '.format(dataName, numberOfColumnsFound, headerCount))
            quarantinedDataFlow = dataFlow.drop_nulls(dataFlowColumns[headerCount:])
            
            # There is a chance we have an extra column but no rows to quarantine, so check this first
            if quarantinedDataFlow.row_count is None:
                quarantinedRowCount = 0 
                print('{0}: no rows to quarantine'.format(dataName))
            else:
                quarantinedRowCount = quarantinedDataFlow.row_count
                # Finally save the data flow so it can be used later
                fullPackagePath = saveDataFlowPackage(quarantinedDataFlow, dataName, thisStageNumber, 'B')
                print('{0}: quarantined {1} rows of data to {2}'.format(dataName, quarantinedRowCount, fullPackagePath))

            # Now filter out the quarantined rows from the main data set
            # NOTE : can't figure out a better way of doign this for now - see note below...
            for columnToCheck in dataFlowColumns[headerCount:]:
                # NOTE - don't know why commented line of code below doesn't work!
                # dataFlow = dataFlow.filter(dataFlow[columnToCheck] != '')
                dataFlow = dataFlow.assert_value(columnToCheck, value != '' , error_code='ShouldBeNone')
                dataFlow = dataFlow.filter(col(columnToCheck).is_error())
                print('{0}: filtered column {1}, row count now {2}'.format(dataName, columnToCheck, dataFlow.row_count))
                
            # Finally drop the extra columns
            dataFlow = dataFlow.drop_columns(dataFlowColumns[headerCount:])
            print('{0}: dropped {1} unwanted columns'.format(dataName, len(dataFlowColumns[headerCount:])))
        else:
            print('{0}: we have found {1} columns, expected {2} so not going to do anything'.format(dataName, numberOfColumnsFound, headerCount))
    
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
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'QuarantineExtraColumns', quarantineRows)

#%%
dataFlowInventoryAll    