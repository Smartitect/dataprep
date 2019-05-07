#%% [markdown]
# # Data Profiling Playbook
# The intention of this notebook is to develop a generic approach to profiling a single column.
# This will take into account:
# - The primary data type of the column - eg if it's a date, we can generate more specialised analytics
# - We may also be able to apply more specialised checks, for example based on REGEX to check national insurance numbers etc.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import seaborn as sns
import pandas_profiling as pp
import datetime
from datetime import datetime
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonPackageHandling import openDataFlowPackage

#%%
dataFlow, fullPackagePath = openDataFlowPackage('UPMFOLDER_UPMPERSON', '60', 'A')

#%%
dataFlow.head(10)

#%%
builder = dataFlow.builders.split_column_by_example('ADDRESS')

#%%
builder.preview()

#%%
builder.keep_delimiters = False

#%%
builder.delimiters = r'\0d0a'

#%%
dataProfile = dataFlow.get_profile()
dataProfile

#%%
newDataFlow = dataFlow.filter(dataFlow['ID'] == 17977)

#%%
newDataFlow.head(10)