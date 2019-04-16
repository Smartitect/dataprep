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
dataFlow = Dataflow.open('./packages/PEOPLE/2/PEOPLE_A_package.dprep')

#%%
dataProfile = dataFlow.get_profile()

#%%
dataProfile

#%%
dataColumns = dataProfile.columns.keys()

#%%
for c in dataColumns:
    print('Column {0} : value count {1}'.format(c, len(dataProfile.columns[c].value_counts)))
    print(dataProfile.columns[c].value_counts)


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
dateOfBirth = df[['ID','DOB','TITLE','MTST']]

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