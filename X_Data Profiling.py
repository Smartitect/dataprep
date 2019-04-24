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
from commonCode import savePackage, openPackage, createFullPackagePath
from commonPackageHandling import openDataFlowPackage

#%%
dataFlow = openDataFlowPackage('PEOPLE', '22', 'A')

#%%
dataFlow.head(10)

#%%
dataProfile = dataFlow.get_profile()
dataProfile

#%%
dataFlow.row_count

#%%
builder = dataFlow.builders.set_column_types()

#%%
builder.learn()

#%%
builder.conversion_candidates

#%%
builder.ambiguous_date_columns

#%%
dataFlow = builder.to_dataflow()

#%%
# Not used fo now, but should be driven by:
stageNumber = '1'
dataName = 'PEOPLE'
qualityFlag = 'A'
noMissingFlag = True

# For now, I'm cheating, just specifying file.  But will use helper function to build ultimately:
dataFlowFile = './packages/PEOPLE/23/PEOPLE_A_package.dprep'
indexColumn = 'ID'
targetColumn = 'DOB'
#%%
dataFlow = Dataflow.open(dataFlowFile)

#%%
dataFlow = dataFlow.distinct(['ID'])

#%%
dataFlow = dataFlow.distinct(['NINO'])


#%%
dataFlow = dataFlow.keep_columns([indexColumn, targetColumn])

#%%
dataProfile = dataFlow.get_profile()

#%%
dataProfile

#%%
columnDataProfile = dataProfile.columns[targetColumn]

#%%
columnDataProfile

#%%
if columnDataProfile.type == 'FieldType.DATE':
    print('Date Field Detected!')
    # NOTE - what do I need to do to detect date field?

#%%
# Add year
columnByExampleBuilder = dataFlow.builders.derive_column_by_example(source_columns = [targetColumn], new_column_name = 'Year')
columnByExampleBuilder.add_example(source_data = {targetColumn : '2008-10-25 00:00:00'}, example_value = '2008')
columnByExampleBuilder.preview()
dataFlow = columnByExampleBuilder.to_dataflow()

#%%
# Add month
columnByExampleBuilder = dataFlow.builders.derive_column_by_example(source_columns = [targetColumn], new_column_name = 'Month')
columnByExampleBuilder.add_example(source_data = {targetColumn : '2008-10-25 00:00:00'}, example_value = 'October')
columnByExampleBuilder.preview()
dataFlow = columnByExampleBuilder.to_dataflow()

#%%
# Add day of month
columnByExampleBuilder = dataFlow.builders.derive_column_by_example(source_columns = [targetColumn], new_column_name = 'DayOfMonth')
columnByExampleBuilder.add_example(source_data = {targetColumn : '2008-10-25 00:00:00'}, example_value = '25')
columnByExampleBuilder.preview()
dataFlow = columnByExampleBuilder.to_dataflow()

#%%
# Add day of week
columnByExampleBuilder = dataFlow.builders.derive_column_by_example(source_columns = [targetColumn], new_column_name = 'DayOfWeek')
columnByExampleBuilder.add_example(source_data = {targetColumn : '2008-10-25 00:00:00'}, example_value = 'Saturday')
columnByExampleBuilder.preview()
dataFlow = columnByExampleBuilder.to_dataflow()

#%%
dataProfile = dataFlow.get_profile()

#%%
dataProfile

#%%
dataColumns = dataProfile.columns.keys()

#%%
dataColumns

#%%
for c in dataColumns:
    valueCounts = dataProfile.columns[c].value_counts
    if valueCounts == None:
        valueCountString = 'None'
    else:
        valueCountString = len(valueCounts)
    print('Column {0} : value count {1}'.format(c, valueCountString))


#%%

#%%
def plotValueCounts(dataProfile,columnName):
    valueCounts = dataProfile.columns[columnName].value_counts
    if valueCounts != None:
        valueCountsDataFrame = pd.DataFrame(columns = ['ColumnName', 'Value', 'Count'])
        for i in valueCounts:
            valueCountsDataFrame = valueCountsDataFrame.append({ \
                'Column' : c, \
                'Value' : i.value, \
                'Count' : i.count}, \
                ignore_index = True)
        if len(valueCountsDataFrame) > 40:
            plot = sns.barplot(x='Value', y='Count', data=valueCountsDataFrame.head(20))
            plot
            plot = sns.barplot(x='Value', y='Count', data=valueCountsDataFrame.tail(20))
            plot
        else:
            plot = sns.barplot(x='Value', y='Count', data=valueCountsDataFrame)
            plot
    return plot

#%%
plot = plotValueCounts(dataProfile, 'Year')

#%%
plot

#%%
plot = plotValueCounts(dataProfile, 'Month')

#%%
plot

#%%
plot = plotValueCounts(dataProfile, 'Day')

#%%
plot

#%%
dataProfileValues = dataProfile.columns.values()

#%%
dataProfileValues
#%%
df = dataFlow.to_pandas_dataframe()

#%%
df = df.sort_values([targetColumn]).reset_index(drop=True)

#%%
df

#%%
profileReport = pp.ProfileReport(df, check_correlation = False)
profileReport.to_file('./profileReport.html')

#%%
plot = sns.countplot(x="Year", data=df)

#%%
plot