
#%% [markdown]
# # Stage 2 - Ingest All Files
# The purpose of this stage is to load all data successfully into "data frames":
# - Attempt to load all of the data;
# - No transforms or clean up steps performed;
# - Get high level statistics such as column and row counts.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, getTableStats

# Let's also set up global variables and common functions...

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

#%%
# Load in file names to be processed from the config.csv file
# NOTE - need to think about a taxonomy for the inventory and data files...
dataFiles = dprep.read_csv('dataFileInventory_02_In.csv').to_pandas_dataframe()

#%%
# The inventory of files that we are going to process...
dataFiles

#%% [markdown]
#---
## Load file:
# Step through each file in the dataFiles02In.csv inventory:
# - Read the first header row of each file and determine exepcted number of columns;
# - Load the CSV data;
# - Count the actual number of columns;
# - Count the actual number of rows;
# - Try to detect data types in each column using **column types builder**;
# - Save the data flow that has been created for each file away so that it can be referenced and used later on.

#%%
# First a quick pass through each file to grab the number of headers and count columns
# NOTE - this loop could be consolidated into main loop
headerCount = []
for index, row in dataFiles.iterrows():
    firstRow = open(row["FullFilePath"]).readline().strip()
    regexPattern = re.compile(',\w')
    patternCount = len(re.findall(regexPattern,firstRow))
    headerCount.append(patternCount + 1)
    print(firstRow)
    print(patternCount)
headerCountCol = pd.DataFrame({'HeaderCount':headerCount})
dataFiles = pd.concat([dataFiles, headerCountCol], axis=1)

#%%
# Main loop now to step through each file and load it...
columnCountList = []
rowCountList = []
packageNameList = []
dataInventoryAllTables = pd.DataFrame()
for index, row in dataFiles.iterrows():

    dataName = row["DataName"]
    fullFilePath = row["FullFilePath"]
    headerCount = row["HeaderCount"]
    
    # Load each file
    print('{0}: loading data from file path {1}'.format(dataName, fullFilePath))
    dataFlow = dprep.read_csv(fullFilePath)

    # Count the rows...
    rowCount = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCount))
    rowCountList.append(rowCount)

    # Get a list of the columns and count them...
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    for i in dataFlowColumns:
        print(i)
    columnCount = len(dataFlowColumns)
    columnCountList.append(columnCount)

    # Capture number of columns found...
    print('{0}: found {1} columns, expected {2}'.format(dataName, columnCount, headerCount))
    
    # Profile the table
    dataProfile = dataFlow.get_profile()
    dataInventory = getTableStats(dataProfile, dataName, '02')

    dataInventoryAllTables = dataInventoryAllTables.append(dataInventory)
    
    # Finally save the data flow so it can be used later
    fullPackagePath = savePackage(dataFlow, dataName, '2', 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)


#%%
# Capture the stats
columnCountCol = pd.DataFrame({'ColumnCountStage02':columnCountList})
dataFiles = pd.concat([dataFiles, columnCountCol], axis=1)

rowCountCol = pd.DataFrame({'RowCountStartStage02':rowCountList})
dataFiles = pd.concat([dataFiles, rowCountCol], axis=1)

packageNameCol = pd.DataFrame({'PackageNameStage02':packageNameList})
dataFiles = pd.concat([dataFiles, packageNameCol], axis=1)

#%%
dataFiles.insert(len(dataFiles.columns), 'RemoveFirstRow', 'Yes')
dataFiles.insert(len(dataFiles.columns), 'ParseNullString', 'Yes')

#%%
# A summary of what we've managed to achieve at the end og this stage
dataFiles

#%%
# Write the inventory out for the next stage in the process to pick up
dataFiles.to_csv('dataFileInventory_02_Out.csv', index = None)

#%%
dataInventoryAllTables



