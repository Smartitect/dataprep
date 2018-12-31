#%% [markdown]
# # Stage 2 - Manipulate MEMBERS
# Let's pick up with the **MEMBERS** package we prepared earlier.

#%%
# Import common variables 
from commonCode import *
from azureml.dataprep import Package

#%%
fullPackagePath = packagePath + '/' + 'members' + packageFile
packageToOpen = Package.open(fullPackagePath)
membersDataFlow = packageToOpen['MEMBERS']

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