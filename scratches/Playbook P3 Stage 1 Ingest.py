
#%% [markdown]
# # Playbook for P3 System : Stage 1 : Ingest

# ## Common
# So this is where we are trying to do all the common stuff to ingest all of the files.  Key is recognition that there are common patterns we can exploit across the files.
# NOTE - still to figure out how to do this from a single file and import it successfully.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from commonCode import savePackage, openPackage, createFullPackagePath

# Let's also set up global variables and common functions...

# Path to the source data
# Unltimately this will be a path to blob or ADLS
# Previous stage will have unencrypted, unzipped and unpacked files ready to be picked up
# Partitioned by client so path would be something like [client]/[date of cut]/upackedData
dataPath = "./data"

# Path to inventory
# Assume same 
inventoryPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

#%% [markdown]
# ## Prepare for ingestion...

#%%
# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFiles.csv').to_pandas_dataframe()

# Create a fully qualified path to the data files and append this to the dataFiles data frame
fullFilePaths = dataPath + '/' + dataFiles.FileName
fullFilePaths.name = "FullFilePath"
dataFiles = pd.concat([dataFiles, fullFilePaths], axis=1)

#%% [markdown]
#---
## Load and perform common processing on each data file
# Step through each file in the config.csv file to extract and do a basic clean up:
# - Load the CSV data;
# - Replace the custom string `<null>` representing a null and any other empty cells to a real `null`;
# - Remove the first row;
# - X Quarantine rows (extract them and put them into a parallel data flow so that they can be fixed at a later stage) which have values in columns that are not listed in the header record;
# - X Drop any columns that we weren't expecting
# - Try to detect data types in each column using **column types builder**
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

#%%
for index, row in dataFiles.iterrows():

    dataName = row["DataName"]
    fullFilePath = row["FullFilePath"]
    # columnCount = row["ColumnCount"]

    # Load each file
    print('{0}: loading data from file path {1}'.format(dataName, fullFilePath))
    dataFlow = dprep.read_csv(fullFilePath)
    print('{0}: loaded {1} rows'.format(dataName, dataFlow.row_count))
    
    # Get a list of the columns
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    
    # Now clean up the data in those columns
    # print('{0}: cleaning up {1} columns (expected {2})'.format(dataName, len(dataFlowColumns), columnCount))
    
    # Replace any instances of empty cells of occurences of the <null> string
    dataFlow = dataFlow.replace_na(dataFlowColumns, custom_na_list='<null>')
    
    # Remove the first row
    # NOTE : future modofication - it would be good to add check to this to make sure it is the blank row we anticipate that begins `SCHEME=AR` 
    dataFlow = dataFlow.skip(1)
    print('{0}: removed first row, down to {1} rows'.format(dataName, dataFlow.row_count))
    
    # Detect and apply column types
    builder = dataFlow.builders.set_column_types()
    builder.learn()
    builder.ambiguous_date_conversions_keep_month_day()
    dataFlow = builder.to_dataflow()
    
    # Finally save the data flow so it can be used later
    fullPackagePath = savePackage(dataFlow, dataName, '1', 'A')
    print('{0}: saved package to {1}'.format(dataName, fullPackagePath))

    # Capture rows for column inventory
    # Use Pandas profile?

# Write out column inventory

# Write out file / table inventory

##%


