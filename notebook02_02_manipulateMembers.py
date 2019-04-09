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
from commonCode import savePackage, openPackage, createFullPackagePath

# Path to the source data
dataPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"
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