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
import seaborn as sns
import pandas_profiling as pp
import datetime
from datetime import datetime
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, getTableStats

# Let's also set up global variables and common functions...
stageNumber = '24'
previousStageNumber = str(int(stageNumber) - 1)
nextStageNumber = str(int(stageNumber) + 1)

#%%
# Load in dataFlow controllder
dataFlows = dprep.read_csv('dataFlowController.csv').to_pandas_dataframe()

#%%
# Output the inventory at this stage...
dataFlows

#%%
dataInventoryAllTables = pd.DataFrame()
packageNameList = []
for index, row in dataFlows.iterrows():

    dataName = row["DataName"]
    operationFlag = row["RemoveDuplicates"]

    # Create a full path the package we should be picking up as the starting point...
    sourcePackagePath = createFullPackagePath(dataName, previousStageNumber, 'A')

    # Open each data flow as saved by the previous stage
    print('{0}: loading data from file path'.format(dataName))
    dataFlow = openPackage(dataName, previousStageNumber, 'A')

    if dataFlow:
        if operationFlag != '':
            # Do the operation...
            print('{0}: doiung the operation on the file'.format(dataName))

        # Profile the table
        print('{0}: generating profile'.format(dataName))
        dataProfile = dataFlow.get_profile()
        dataInventory = getTableStats(dataProfile, dataName, stageNumber)
        dataInventoryAllTables = dataInventoryAllTables.append(dataInventory)

        # Finally save the data flow so it can be used later
        targetPackagePath = savePackage(dataFlow, dataName, stageNumber, 'A')
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))
    else:
        print('{0}: no package file found.'.format(dataName))

#%%
dataInventoryAllTables.to_csv('columnInventory_' + stageNumber + '.csv', index = None)

#%%
dataFlow = Dataflow.open(dataFlowFile)

#%%
dataProfile = dataFlow.get_profile()
dataProfile

#%%
for i in targetColumns:
    dataFlow = dataFlow.distinct(i)

#%% [markdown]
# NOTE - we need to write some code here that will:
# - Generate some documentation as part of the notebook to expose the duplicate records (eg some kind of group by + head)
# - Quarantine the duplicate rows that have been removed for audit purposes

#%%
dataProfile = dataFlow.get_profile()
dataProfile