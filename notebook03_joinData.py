#%% [markdown]
# Stage 3 - Join Data

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
import validators
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
# ## Open PEOPLE and MEMBERS data flows from stage 2
# Simply pick up the data flows from stage 2...

#%%
peopleDataFlow = openPackage('PEOPLE', '2', 'A')
membersDataFlow = openPackage('MEMBERS', '2', 'A')

#%% [markdown]
# ## Join the PEOPLE and MEMBERS data flows
# Crunch time!  Let's see if we can get these cleaned up data sets to join.

#%%
join_builder = peopleDataFlow.builders.join(right_dataflow=membersDataFlow, left_column_prefix='l', right_column_prefix='r')
join_builder.detect_column_info()
join_builder

#%%
join_builder.generate_suggested_join()
join_builder.list_join_suggestions()

#%% [markdown]
# Weird, it doesn't come up with a suggestion despite having two MEMNO integer columns to work with!

#%%
joinedDataFlow = dprep.Dataflow.join(left_dataflow=peopleDataFlow,
                                      right_dataflow=membersDataFlow,
                                      join_key_pairs=[('ID', 'PEOPLEID')],
                                      left_column_prefix='PEOPLE_',
                                      right_column_prefix='MEMBERS_')

#%%
joinedDataFlow.head(5)

#%%
joinedDataFlow.get_profile()

#%% [markdown]
# Just running a couple of checks now to see how well the join has worked:

#%%
print('PEOPLE row count = {0}'.format(peopleDataFlow.row_count))
print('MEMBERS row count = {0}'.format(membersDataFlow.row_count))
print('JOINED row count = {0}'.format(joinedDataFlow.row_count))

#%%
orphanedPeopleDataFlow = joinedDataFlow.filter(joinedDataFlow['MEMBER_PEOPLEID'] == None)
orphanedPeopleDataFlow.head(20)

#%% [markdown]
# ## Save JOINED data
# Finally save the JOINED data flow that comes out of stage 3 for consumption downstream

#%%
fullPackagePath = savePackage(joinedDataFlow, 'JOINED', '3', 'A')
print('Saved package to file {0}'.format(fullPackagePath))

