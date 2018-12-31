#%% [markdown]
# Stage 3 - Join Data

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

fullPackagePath = packagePath + '/' + 'people' + packageFile
packageToOpen = Package.open(fullPackagePath)
peopleDataFlow = packageToOpen['PEOPLE']

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
