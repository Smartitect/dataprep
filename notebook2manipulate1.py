#%% [markdown]
#---
# # Stage 2 : Manipulate Part 1
# ## PEOPLE
# Let's pick up with the **PEOPLE** package we prepared earlier.

#%%
# Import common variables 
from commonVariables import *
from azureml.dataprep import Package

fullPackagePath = packagePath + '/' + 'people' + packageFile
packageToOpen = Package.open(fullPackagePath)
peopleDataFlow = packageToOpen['PEOPLE']


#%% [markdown]
# Inspect the top 100 rows:
#%%
peopleDataFlow.head(100)

#%% [markdown]
# ### ADDRESS
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
# ### SEX
# Find distinct values:

#%%
peopleDataFlow.get_profile().columns['SEX'].value_counts

#%% [markdown]
# Looks good all either an M or an F, no anomolies or missing values.

#%% [markdown]
# ### TITLE
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
# ### MSTA
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


#%% [markdown]
## Join People and Member Data

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
people_memberJoined = dprep.Dataflow.join(left_dataflow=peopleDataFlow,
                                      right_dataflow=membersDataFlow,
                                      join_key_pairs=[('ID', 'PEOPLEID')],
                                      left_column_prefix='PEOPLE_',
                                      right_column_prefix='MEMBERS_')

#%%
people_memberJoined.head(5)
#%% [markdown]
## Map To Canonical Form
# Map the joined up data onto a canonical form.
# Haven't figured out how best to do this yet!
# It would be great if you could define some canonical form, or create from an existing file / database table schema?
# Then you could apply some ML to infer / learn how to map?

#%% [markdown]
## Run Generic Data Quality
# Now we're at the stage where we can start to apply generic data quality checks...
### Date Checks

#### Check : Date Joined Company is after Date Joined Scheme

#%%
people_memberJoined = people_memberJoined.add_column(new_column_name='DQC_DJS_greaterThan_DJC',
                           prior_column='MEMBERS_DJS',
                           expression=people_memberJoined['MEMBERS_DJS'] > people_memberJoined['PEOPLE_DOB'])


#%%
people_memberJoined = people_memberJoined.new_script_column(new_column_name='test2', insert_after='DQC_DJS_greaterThan_DJC', script="""
def newvalue(row):
    return 'ERROR - DJS earlier than DJC'
""")

#%%
people_memberJoined.get_profile()

#%%
people_memberJoined.head(20)

#%% [markdown]
#### 4.1.2 - Date of Birth is after Date Joined Scheme
