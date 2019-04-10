#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import time
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath

# Let's also set up global variables...
# NOTE - is there any way to set these up centrally?

# Path to the source data
# NOTE - ultimately this could point to other storage sources such as blob on Azure
dataPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

#%% [markdown]
# ## List all of the files in the data directory...

#%%
fileList = os.listdir(dataPath)
dataFiles = pd.DataFrame({'FileName':fileList})

#%%
fullFilePaths = dataPath + '/' + dataFiles.FileName
fullFilePaths.name = "FullFilePath"
dataFiles = pd.concat([dataFiles, fullFilePaths], axis=1)

#%%
fileSize = []
modifiedTime = []
for index, row in dataFiles.iterrows():
    fileSize.append(os.path.getsize(row.FullFilePath))
    modifiedTime.append(time.ctime(os.path.getmtime(row.FullFilePath)))
fileSizeCol = pd.DataFrame({'FileSize':fileSize})
modifiedTimeCol = pd.DataFrame({'Modified':modifiedTime})
dataFiles = pd.concat([dataFiles, fileSizeCol], axis=1)
dataFiles = pd.concat([dataFiles, modifiedTimeCol], axis=1)

#%%
dataFiles

#%%
dataFiles.to_csv('dataFiles01.csv')