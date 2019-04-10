
#%% [markdown]
# # Stage : Ingest 2
# Purpose of this stage is to load all data successfully into data frames

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
from commonCode import savePackage, openPackage, createFullPackagePath

# Let's also set up global variables and common functions...

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFiles02.csv').to_pandas_dataframe()

# now grab the number of headers in the first row of each file
headerCount = []
for index, row in dataFiles.iterrows():
    firstRow = open(row["FullFilePath"]).readline().strip()
    regexPattern = re.compile(',\w')
    patternCount = len(re.findall(regexPattern,firstRow))
    headerCount.append(patternCount + 1)
headerCountCol = pd.DataFrame({'HeaderCount':headerCount})
dataFiles = pd.concat([dataFiles, headerCountCol], axis=1)

#%%
# Output the inventory at this stage...
dataFiles

#%% [markdown]
#---
## Load and perform common processing on each data file
# Step through each file in the config.csv file to extract and do a basic clean up:
# - Load the CSV data;
# - Replace the custom string `<null>` representing a null and any other empty cells to a real `null`;
# - Remove the first row;
# - Quarantine rows (extract them and put them into a parallel data flow so that they can be fixed at a later stage) which have values in columns that are not listed in the header record;
# - Drop any columns that we weren't expecting
# - Try to detect data types in each column using **column types builder**
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

#%%
columnCountList = []
rowCountStartList = []
rowCountEndList = []
quarantinedRowsList = []
packageNameList = []
for index, row in dataFiles.iterrows():

    dataName = row["DataName"]
    fullFilePath = row["FullFilePath"]
    headerCount = row["HeaderCount"]
    removeFirstRow = row["RemoveFirstRow"]
    parseNullString = row["ParseNullString"]

    # Load each file
    print('{0}: loading data from file path {1}'.format(dataName, fullFilePath))
    dataFlow = dprep.read_csv(fullFilePath)

    # Count the rows...
    rowCountStart = dataFlow.row_count
    print('{0}: loaded {1} rows'.format(dataName, rowCountStart))
    rowCountStartList.append(rowCountStart)

    # Get a list of the columns and count them...
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    columnCount = len(dataFlowColumns) + 1
    columnCountList.append(columnCount)

    # Capture number of columns found...
    print('{0}: found {1} columns, expected {2}'.format(dataName, columnCount, headerCount))
    
    if parseNullString == 'Yes':
        # Replace any occurences of the <null> string with proper empty cells...
        dataFlow = dataFlow.replace_na(dataFlowColumns, custom_na_list='<null>')
    
    if parseNullString == 'Yes':
        # Remove the first row...
        # NOTE : future modofication - it would be good to add check to this to make sure it is the blank row we anticipate that begins `SCHEME=AR` 
        dataFlow = dataFlow.skip(1)
        print('{0}: removed first row, down to {1} rows'.format(dataName, dataFlow.row_count))
    
    # Quarantine rows which don't have values in the extra columns
    if headerCount != len(dataFlowColumns):
        # NOTE - this logic assumes that all unwanted columns are on the far right, this could be improved!
        # Fork a new data flow with rows that have data in the un-expected columns
        quarantinedDataFlow = dataFlow.drop_nulls(dataFlowColumns[headerCount:])
        quarantinedRowCount = dataFlow.row_count
        print('{0}: created quarantined data with {1} rows'.format(dataName, quarantinedRowCount))
        quarantinedRowsList.append(quarantinedRowCount)
        # Finally save the data flow so it can be used later
        fullPackagePath = savePackage(dataFlow, dataName, '1', 'B')
        print('{0}: saved quarantined data to {1}'.format(dataName, fullPackagePath))

    # Filter out the quarantined rows from the main data set
    # NOTE : can't figure out a better way of doign this for now - see note below...
    for columnToCheck in dataFlowColumns[headerCount:]:
        # NOTE - don't know why commented line of code below doesn't work!
        # dataFlow = dataFlow.filter(dataFlow[columnToCheck] != '')
        dataFlow = dataFlow.assert_value(columnToCheck, value != '' , error_code='ShouldBeNone')
        dataFlow = dataFlow.filter(col(columnToCheck).is_error())
        print('{0}: filtered column {1}, row count now {2}'.format(dataName, columnToCheck, dataFlow.row_count))
    
    # Count the rows...
    rowCountEnd = dataFlow.row_count
    rowCountEndList.append(rowCountEnd)

    # Now drop the extra columns
    dataFlow = dataFlow.drop_columns(dataFlowColumns[headerCount:])
    print('{0}: dropped {1} unwanted columns'.format(dataName, len(dataFlowColumns[headerCount:])))
    
    # Detect and apply column types
    builder = dataFlow.builders.set_column_types()
    builder.learn()
    builder.ambiguous_date_conversions_keep_month_day()
    dataFlow = builder.to_dataflow()
    
    # Finally save the data flow so it can be used later
    fullPackagePath = savePackage(dataFlow, dataName, '1', 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))
    packageNameList.append(fullPackagePath)


#%%
# Capture the stats
columnCountCol = pd.DataFrame({'ColumnCount':columnCountList})
dataFiles = pd.concat([dataFiles, columnCountCol], axis=1)

rowCountStartCol = pd.DataFrame({'RowCountStart':rowCountStartList})
dataFiles = pd.concat([dataFiles, rowCountStartCol], axis=1)

rowCountEndCol = pd.DataFrame({'RowCountEnd':rowCountEndList})
dataFiles = pd.concat([dataFiles, rowCountEndCol], axis=1)

quarantinedRowsCol = pd.DataFrame({'QuanantinedRows':quarantinedRowsList})
dataFiles = pd.concat([dataFiles, quarantinedRowsCol], axis=1)

packageNameCol = pd.DataFrame({'PackageNameA':packageNameList})
dataFiles = pd.concat([dataFiles, packageNameCol], axis=1)

#%%
dataFiles
