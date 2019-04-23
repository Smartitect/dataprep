#%% [markdown]
# # Stage 1 - Inventory Files
# The purpose of this stage of the process is to create an inventory of all of the source files
# Some future improvements to process:
# - Connect to Azure storage
# - Work with agreed taxonomy for storage so that it is more parameter driven: client/scheme name, data cut version or date etc.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import time
import shutil
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import saveDataFileInventory

# Let's also set up global variables...
# NOTE - is there any way to set these up centrally?
stageNumber = '00'

# Path to the source data
# NOTE - ultimately this could point to other storage sources such as blob on Azure
dataPath = './data'

#%%
# List all of the files in the data directory...
fileList = os.listdir(dataPath)

#%%
# Prepare empty dataframe to store the inventory for the files
dataFileStats = pd.DataFrame(columns = [ \
    'DataName', \
    'FileName', \
    'FullFilePath', \
    'FileSize', \
    'ModifiedTime'])
        
#%%
# Step though each file
for file in fileList:
    
    # Get the stats for each file
    fullFilePath = dataPath + '/' + file
    fileSize = os.path.getsize(fullFilePath)
    modifiedTime = time.ctime(os.path.getmtime(fullFilePath))
    dataName = file.split('.')[0]

    # Append the statistics for that file as a new row in the dataframe
    dataFileStats = dataFileStats.append({ \
        'DataName' : dataName, \
        'FileName' : file, \
        'FullFilePath' : fullFilePath, \
        'FileSize' : fileSize, \
        'ModifiedTime' : modifiedTime}, ignore_index = True)

#%%
# Create folders to use as workspaces for each of the files
# for index, row in dataFiles.iterrows():
#    if not os.path.isdir(packagePath):
#        os.mkdir(packagePath)

#    if os.path.isdir(packagePath + row.DataName):
#        shutil.rmtree(packagePath + row.DataName)

#    os.mkdir(packagePath + row.DataName)

#%%
# Write inventory away
dataFileStats.to_csv('dataFileInventory_' + stageNumber + '.csv', index = None)

#%%
dataFileStats