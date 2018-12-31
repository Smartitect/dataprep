#%% [markdown]
#---
# # Stage 2 : Manipulate PEOPLE
# Let's pick up with the **PEOPLE** package we prepared earlier.

#%%
# Import common variables 
from commonCode import *
from azureml.dataprep import Package

fullPackagePath = packagePath + '/' + 'people' + packageFile
packageToOpen = Package.open(fullPackagePath)
peopleDataFlow = packageToOpen['PEOPLE']


#%% [markdown]
# Inspect the top 100 rows:
#%%
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
# - No missing values for DOB : date of birth
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
# Looks good all either an M or an F, no anomolies or missing values.

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
# ## MEMBER
# Let's take a look at the **MEMBERS.csv** file.
# ### Ingest MEMBER
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


