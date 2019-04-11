#%% [markdown]
# # Stage 1 - Inventory Files
# The purpose of this stage of the process is to create an inventory of all of the source files
# Some future improvements to process:
# - Connect to Azure storage
# - Work with agreed taxonomy for sotrage so that it is more parameter driven: client/scheme name, data cut version or date etc.

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

# Let's also set up global variables...
# NOTE - is there any way to set these up centrally?
stageNumber = '1'

# Path to the source data
# NOTE - ultimately this could point to other storage sources such as blob on Azure
dataPath = "./data"

#%%
# List all of the files in the data directory...
fileList = os.listdir(dataPath)
dataFiles = pd.DataFrame({'FileName':fileList})

#%%
fullFilePaths = dataPath + '/' + dataFiles.FileName
fullFilePaths.name = "FullFilePath"
dataFiles = pd.concat([dataFiles, fullFilePaths], axis=1)

#%%
fileSize = []
modifiedTime = []
dataNames = []
for index, row in dataFiles.iterrows():
    fileSize.append(os.path.getsize(row.FullFilePath))
    modifiedTime.append(time.ctime(os.path.getmtime(row.FullFilePath)))
    dataNames.append(row.FileName.split('.')[0])

fileSizeCol = pd.DataFrame({'FileSize':fileSize})
modifiedTimeCol = pd.DataFrame({'Modified':modifiedTime})
dataNamesCol = pd.DataFrame({'DataName':dataNames})
dataFiles = pd.concat([dataFiles, fileSizeCol], axis=1)
dataFiles = pd.concat([dataFiles, modifiedTimeCol], axis=1)
dataFiles = pd.concat([dataFiles, dataNamesCol], axis=1)

#%%
dataFiles

#%%
# Create folders to use as workspaces for each of the files
for index, row in dataFiles.iterrows():
    if os.path.isdir('./packages/' + row.DataName):
        shutil.rmtree('./packages/' + row.DataName)

    os.mkdir('./packages/' + row.DataName)

#%%
# Write inventory away as input for next stage in the process
dataFiles.to_csv('dataFileInventory_' + stageNumber + '_Out.csv', index = None)


nextStageNumber = str(int(stageNumber) + 1)
dataFiles.to_csv('dataFileInventory_' + nextStageNumber + '_In.csv', index = None)
