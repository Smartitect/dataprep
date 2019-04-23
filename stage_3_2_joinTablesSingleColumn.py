#%% [markdown]
# # Join Tables
# To join two tables together:
# - We are using the full prescriptive method
# - Column names are appended with parent table name

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
from commonCode import savePackage, openPackage, createFullPackagePath

#%%
# For now, I'm cheating, just specifying file.  But will use helper function to build ultimately:
leftDataFlowFile = './packages/PEOPLE/23/PEOPLE_A_package.dprep'
leftDataFlowTable = 'PEOPLE'
leftDataFlowJoinColumn = 'ID'
rightDataFlowFile = './packages/MEMBERS/23/MEMBERS_A_package.dprep'
rightDataFlowTable = 'MEMBERS'
rightDataFlowJoinColumn = 'PEOPLEID'

#%%
leftDataFlow = Dataflow.open(leftDataFlowFile)
rightDataFlow = Dataflow.open(rightDataFlowFile)

#%%
leftDataFlowProfile = leftDataFlow.get_profile()
rightDataFlowProfile = rightDataFlow.get_profile()

#%%
leftDataFlowProfile

#%%
rightDataFlowProfile

#%%
print('Table : {0}, Column : {1}, Count : {2}'.format(leftDataFlowTable, leftDataFlowJoinColumn, leftDataFlowProfile.columns[leftDataFlowJoinColumn].count))

#%%
print('Table : {0}, Column : {1}, Count : {2}'.format(rightDataFlowTable, rightDataFlowJoinColumn, rightDataFlowProfile.columns[rightDataFlowJoinColumn].count))

#%%
join_builder = leftDataFlow.builders.join(right_dataflow=rightDataFlow, 
                                        left_column_prefix='l_',
                                        right_column_prefix='r_')


#%%
join_builder.detect_column_info()

#%%
join_builder.generate_suggested_join()

#%%
join_builder.list_join_suggestions()

#%%
join_builder.join_key_pairs=[(leftDataFlowJoinColumn, rightDataFlowJoinColumn)]

#%%
print(join_builder.preview())

#%%
dataFlowJoined = join_builder.to_dataflow()

#%%
dataFlowJoined = dprep.Dataflow.join(left_dataflow=leftDataFlow,
                                      right_dataflow=rightDataFlow,
                                      join_key_pairs=[(leftDataFlowJoinColumn, rightDataFlowJoinColumn)],
                                      left_column_prefix='l_',
                                      right_column_prefix='r_',
                                      join_type='MATCH')

#%%
dataFlowJoinedProfile = dataFlowJoined.get_profile()
dataFlowJoinedProfile