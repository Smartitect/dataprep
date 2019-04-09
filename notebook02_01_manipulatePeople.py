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
#%% [markdown]
# Load the A class PEOPLE data from stage 1 and inspect the top 100 rows:

#%%
peopleDataFlow = openPackage('PEOPLE', '1', 'A')
peopleDataFlow.head(100)

#%% [markdown]
# ## ADDRESS
# There is a need to replace the CR/LF `\0d0a` string with a comma in the address, and replace empty values with an empty string:
#%%
peopleDataFlow = peopleDataFlow.str_replace('ADDRESS', r'\0d0a', ', ')
peopleDataFlow = peopleDataFlow.replace('ADDRESS', r', , , ', None)
peopleDataFlow.head(5)

#%%
peopleDataFlow.get_profile()
#%% [markdown]
# Inspect the progile -so good things to see:
# - No errors in any of the date columns having applied this type to the column
# - One missing value for DOB : date of birth
# - Lots of missing values for DATEDIED which is intuitive
# - Only 203 people with married date, doesn't seem right : MARRDATE
#
### Data Types
# A few datatypes that haven't been picked up automtically

#%%
peopleDataFlow = peopleDataFlow.to_datetime(columns = ['MINRETIREMENTDATE'], date_time_formats = ['%d/%m/%Y'])
peopleDataFlow = peopleDataFlow.to_long(['ID'])

#%% [markdown]
# ## SEX
# Find distinct values:

#%%
peopleDataFlow.get_profile().columns['SEX'].value_counts

#%% [markdown]
# Looks good, apart from one record, all either an M or an F, no anomolies or missing values.
# Let's quarantine the row which is missing this field:

#%%
quarantinedPeopleDataFlow = peopleDataFlow.filter(peopleDataFlow['SEX'] == None)

#%% [markdown]
# ## TITLE
# Find distinct values:

#%%
peopleDataFlow.get_profile().columns['TITLE'].value_counts

#%% [markdown]
# Here we can see that there are a number of different ways of representing the likes of "Mrs", "Mrs.", "mrs"
# Lets see what fuzzy grouping can do:

#%%
builder = peopleDataFlow.builders.fuzzy_group_column(source_column='TITLE',
                                       new_column_name='TITLE_grouped',
                                       similarity_threshold=0.8,
                                       similarity_score_column_name='TITLE_grouped_score')

builder.learn()
builder.groups

#%% [markdown]
# First pass with a threashold of 0.8 doesn't do so well, so let's try with a threshold of 0.9:
#%%
builder.similarity_threshold = 0.9
builder.learn()
builder.groups

#%% [markdown]
# So this cleans up the instances of "Mrs", "Mrs." and "mrs" + "Rev" and "REV"

#%%
peopleDataFlow = builder.to_dataflow()
peopleDataFlow.get_profile().columns['TITLE_grouped'].value_counts

#%% [markdown]
# Tidy up the loose ends now
#%%
peopleDataFlow = peopleDataFlow.replace('TITLE_grouped', 'Sist', 'Sister')
peopleDataFlow = peopleDataFlow.replace('TITLE_grouped', 'Sis', 'Sister')
peopleDataFlow = peopleDataFlow.replace('TITLE_grouped', 'M', None)
peopleDataFlow.get_profile().columns['TITLE_grouped'].value_counts

#%% [markdown]
# ## MSTA - Marital Status
# Find distinct values:

#%%
peopleDataFlow.get_profile().columns['MSTA'].value_counts

#%% [markdown]
# Here we can see that there are a number of different ways of representing the likes of "Mrs", "Mrs.", "mrs"
# Let's see what fuzzy grouping can do:

#%%
builder = peopleDataFlow.builders.fuzzy_group_column(source_column='MSTA',
                                       new_column_name='MSTA_grouped',
                                       similarity_threshold=0.1,
                                       similarity_score_column_name='MSTA_grouped_score')

builder.learn()
builder.groups

#%% [markdown]
# So this cleans up the instances of "Serarated" and "Seperated", but despite placing the threashold as low as 0.1 it has not picked up the other example such as those around "Widowed" or "Civil Partner"

#%%
peopleDataFlow = builder.to_dataflow()
peopleDataFlow.get_profile().columns['MSTA_grouped'].value_counts

#%%
peopleDataFlow = peopleDataFlow.replace('MSTA_grouped', 'Unknown', None)
peopleDataFlow = peopleDataFlow.replace('MSTA_grouped', 'Widow/Wido', 'Widowed')
peopleDataFlow = peopleDataFlow.replace('MSTA_grouped', '0', None)
peopleDataFlow = peopleDataFlow.replace('MSTA_grouped', 'Civil Part', 'Civil Partner')
peopleDataFlow = peopleDataFlow.replace('MSTA_grouped', '1', None)

#%%
peopleDataFlow.get_profile().columns['MSTA_grouped'].value_counts



#%% [markdown]
# ## Save PEOPLE data
# Finally we'll save away what we've created so that it can be picked up later on in the process.
#%%
fullPackagePath = savePackage(peopleDataFlow, 'PEOPLE', '2', 'A')
print('Saved package to file {0}'.format(fullPackagePath))

