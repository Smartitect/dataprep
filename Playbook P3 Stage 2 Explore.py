#%% [markdown]
#---
# # Stage 2 : Manipulate PEOPLE
# Let's pick up with the **PEOPLE** package we prepared earlier in stage 1.

# ## Common
# So this is where we are trying to do all the common stuff to ingest all of the files.  Key is recognition that there are common patterns we can exploit across the files.
# NOTE - still to figure out how to do this from a single file and import it successfully.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import pandas_profiling as pp
import azureml.dataprep as dprep
# import seaborn as sns
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

#%% [markdown]
# Load the A class PEOPLE data from stage 1 and inspect the top 100 rows:

#%%
peopleDataFlow = openPackage('PEOPLE', '1', 'A')
peopleDataFlow.head(100)

#%%
peopleDataFlow.get_profile()
#%% [markdown]
# ## Save PEOPLE data
# Finally we'll save away what we've created so that it can be picked up later on in the process.
#%%
# fullPackagePath = savePackage(peopleDataFlow, 'PEOPLE', '2', 'A')
# print('Saved package to file {0}'.format(fullPackagePath))

#%%
df = peopleDataFlow.to_pandas_dataframe()
pandas_profiling.profilereport(df)