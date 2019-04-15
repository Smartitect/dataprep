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
import shutil
from azureml.dataprep import value, ReplacementsValue
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonCode import savePackage, openPackage, createFullPackagePath, getTableStats, saveColumnInventoryForTable

#%%
sourceFileName = 'USERDATA'
lookupFileName = 'LOOKUPS'
previousStageNumber = '2'
stageNumber = '3'

#%%
dataFiles = dprep.read_csv('dataFileInventory_' + stageNumber + '_In.csv').to_pandas_dataframe()
dataFlow = Dataflow.open('./packages/' + sourceFileName + '/' + previousStageNumber + '/' + sourceFileName +'_A_package.dprep')
lookupsDataFlow = Dataflow.open('./packages/' + lookupFileName + '/' + previousStageNumber + '/' + lookupFileName + '_A_package.dprep')

#%%
dataFlow = dataFlow.map_column('FORM', 'FORM2', {ReplacementsValue('EXITDEFERRED', 'EXITDEFERREDS'), ReplacementsValue('EXITRETIRED', 'EXITRETIREMENT'), ReplacementsValue('EXITDEFERRED', 'EXITDEFERREDS')})
dataFlow = dataFlow.drop_columns('FORM')
dataFlow = dataFlow.rename_columns({'FORM2': 'FORM'})

#%%
dataProfile = dataFlow.get_profile()
rowCount = dataFlow.row_count

newRowCounts = []
#%%
for col in dataProfile.columns['FORM'].value_counts:
    newDataFlow = dataFlow.filter(dataFlow['FORM'] == col.value)

    filteredLookupsDataFlow = lookupsDataFlow.filter((lookupsDataFlow['LOOKTYPE'] == 'U') & (lookupsDataFlow['LFIELD'] == col.value))
    filteredLookupsDataFlow.head(100)
    #joinedDataFlow = dprep.Dataflow.join(left_dataflow=newDataFlow, right_dataflow=lookupsDataFlow, join_key_pairs=[('FORM', 'LFIELD'), ('')]
    
    lookupsDataFrame = filteredLookupsDataFlow.to_pandas_dataframe()

    replacedColumns = []

    for index, row in lookupsDataFrame.iterrows():
        partitionOccurred = True
        originalValue = row['COMPNAME']
        replacementValue = row['LDESC']

        replacedColumns.append(replacementValue)
        newDataFlow = newDataFlow.rename_columns({originalValue: replacementValue})

    newDataProfile = newDataFlow.get_profile()

    for column in newDataProfile.columns.values():
        if column.min is None and column.max is None:
            newDataFlow = newDataFlow.drop_columns(column.name)

    newDataFlow.head(5)

    if partitionOccurred:
        packageName = sourceFileName + '_' + col.value

        if os.path.isdir('./packages/' + packageName):
            shutil.rmtree('./packages/' + packageName)

        os.mkdir('./packages/' + packageName)

        # builder = newDataFlow.builders.set_column_types()
        # builder.ambiguous_date_conversions_drop()
        # builder.learn()
        # newDataFlow = builder.to_dataflow()

        savePackage(newDataFlow, packageName, '2', 'A')

        # Profile the table
        newDataProfile = newDataFlow.get_profile()
        newRowCounts.append(newDataFlow.row_count)

        dataInventory = getTableStats(newDataProfile, packageName, stageNumber)
        saveColumnInventoryForTable(dataInventory, packageName, stageNumber)
    
#%%
for col in dataProfile.columns['FORM'].value_counts:
    packageName = sourceFileName + '_' + col.value

    lookupsDataFlow = Dataflow.open('./packages/' + packageName + '/2/' + packageName+ '_A_package.dprep')

    print(lookupsDataFlow.get_profile())
    print(lookupsDataFlow.head(1000))

#%%
lookupsDataFlow = Dataflow.open('./packages/USERDATA_BASIC_DATA/2/USERDATA_BASIC_DATA_A_package.dprep')
lookupsDataFlow.head(10000)


dataFiles.to_csv('dataFileInventory_' + stageNumber + '_Out.csv', index = None)

