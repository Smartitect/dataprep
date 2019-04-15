#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import seaborn as sns
import pandas_profiling
import datetime
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath

#%%
dataFlow = Dataflow.open('./packages/MEMBERS/2/MEMBERS_A_package.dprep')

#%%
dataProfile = dataFlow.get_profile()

#%%
columnStats = pd.DataFrame(columns = [ \
    'DataName', \
    'Stage', \
    'ColumnName', \
    'Type', \
    'Min', \
    'Max', \
    'RowCount', \
    'MissingCount', \
    'NotMissingCount', \
    'ErrorCount', \
    'EmptyCount', \
    'Mean', \
    'ValueCount'])

#%%
for item in dataProfile.columns.values():
    if item.value_counts == None:
        valueCount = None
    else:
        valueCount = len(item.value_counts)
    columnStats = columnStats.append({ \
    'DataName' : 'TABLE', \
    'Stage' : '02', \
    'ColumnName' : item.column_name, \
    'Type' : item.type, \
    'Min' : item.min, \
    'Max' : item.max, \
    'RowCount' : item.count, \
    'MissingCount' : item.missing_count, \
    'NotMissingCount' : item.not_missing_count, \
    'ErrorCount' : item.error_count, \
    'EmptyCount' : item.empty_count, \
    'Mean' : item.mean, 'ValueCount' : valueCount}, \
    ignore_index = True)
columnStats.insert(2, 'DateTime', datetime.datetime.now())

#%%
columnStats

#%%
for c in dataProfile.columns.keys():
    print(dataProfile.columns[c].value_counts)
#%%
dataProfile

#%%
dataColumns = dataProfile.columns.keys()

#%%
dataProfileValues = dataProfile.columns.values()


#%%
columnStats = pd.DataFrame(columns = ['DataName', 'Stage', 'DateTIme', 'ColumnName', 'Type', 'Min', 'Max', 'RowCount', 'MissingCount', 'NotMissingCount', 'ErrorCount', 'EmptyCount', 'Mean'])

for item in dataProfileValues:
    print (item.column_name)
    columnStats = columnStats.append({'DataName' : 'X', 'Stage' : 'Y', 'DateTIme' : 'Z', 'ColumnName' : item.column_name, 'Type' : item.type, 'Min' : item.min, 'Max' : item.max, 'RowCount' : item.count, 'MissingCount' : item.missing_count, 'NotMissingCount' : item.not_missing_count, 'ErrorCount' : item.error_count, 'EmptyCount' : item.empty_count, 'Mean' : item.mean}, ignore_index = True)



#%%
df = dataFlow.to_pandas_dataframe()
#%%
dateOfBirth = df[['ID','DOB']]

#%%
profileReport = pp.ProfileReport(dateOfBirth, check_correlation = True)
profileReport.to_file('./profileReport.html')

#%%
dateOfBirth['DOB'] = pd.to_datetime(dateOfBirth['DOB'])

#%%
dateOfBirth['Year'] = dateOfBirth['DOB'].dt.year
dateOfBirth['Month'] = dateOfBirth['DOB'].dt.month
dateOfBirth['Day'] = dateOfBirth['DOB'].dt.day
dateOfBirth['WeekDay'] = dateOfBirth['DOB'].dt.dayofweek
#%%
dateOfBirth
#%%
dateOfBirth.dtypes

#%%
plot = sns.countplot(x="Year", data=dateOfBirth)