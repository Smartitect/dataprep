#%% [markdown]
# # Stage 2 - Manipulate MEMBERS
# Let's pick up with the **MEMBERS** package we prepared earlier in stage 1.

# ## Common
# So this is where we are trying to do all the common stuff to ingest all of the files.  Key is recognition that there are common patterns we can exploit across the files.
# NOTE - still to figure out how to do this from a single file and import it successfully.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Package

# Let's also set up global variables and common functions...

# Path to the source data
dataPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

# A helper function to create full package path
def createFullPackagePath(packageName, stage, qualityFlag):
    return packagePath + '/' + packageName + '_' + stage + '_' + qualityFlag + packageFileSuffix

# A save package helper function
def savePackage(dataFlowToPackage, packageName, stage, qualityFlag):
    dataFlowToPackage = dataFlowToPackage.set_name(packageName)
    packageToSave = dprep.Package(dataFlowToPackage)
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    packageToSave = packageToSave.save(fullPackagePath)
    return fullPackagePath

# An open package helper function
def openPackage(packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    packageToOpen = Package.open(fullPackagePath)
    dataFlow = packageToOpen[packageName]
    return dataFlow

#%%
membersDataFlow = openPackage('MEMBERS', '1', 'A')

#%%
membersDataFlow.head(100)

#%% [markdown]
# Commentary on data
#%%
membersDataFlow.get_profile()

#%% [markdown]
# Commentary on profile

#%%
membersDataFlow = membersDataFlow.to_long(['PEOPLEID'])

#%% [markdown]
# ## Save MEMBERS data
# Finally save the MEMBERS data flow that comes out of stage 2 for consumption downstream

#%%
fullPackagePath = savePackage(membersDataFlow, 'MEMBERS', '2', 'A')
print('Saved package to file {0}'.format(fullPackagePath))