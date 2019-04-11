#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import seaborn as sns
import pandas_profiling
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath

#%%
dataFlow = Dataflow.open('./packages/PEOPLE_3_A_package.dprep')

#%%
df = dataFlow.to_pandas_dataframe()

#%%
dataProfile = dataFlow.get_profile()

#%%
dataProfile

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