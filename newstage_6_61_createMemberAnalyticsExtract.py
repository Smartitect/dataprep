#%% [markdown]
# # Stage : Create a "dimension table" against whicht to perform analysis and enable slicing and dicing of data validations run against the membership of the scheme
# Requires UPMFOLDER_UPMPERSON join to have occurred and data populated in the table
#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import col, ReplacementsValue
from azureml.dataprep import Dataflow
from commonDataFlowProcessingLoop import dataFlowProcessingLoop
from commonInventoryCreation import getColumnStats, getDataFlowStats
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage, createNewPackageDirectory
from mappingCode import load_transformation_configuration, get_lookups_from_transforms, get_destination_column_name

# Let's also set up global variables and common functions...
previousStageNumber = '60'
thisStageNumber = '61'
qualityFlag = 'A'
dataName = 'UPMFOLDER_UPMPERSON'
dataAnalyticsPath = './dataAnalytics'

#%%
# Open the data flow package that has been prepared...
dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

# Now convert it to a pandas dataframe...
dataFrame = dataFlow.to_pandas_dataframe()

def createFilePath(dataAnalyticsPath, dataName, stage, qualityFlag):

    if not os.path.isdir(dataAnalyticsPath):
        os.mkdir(dataAnalyticsPath)

    return dataAnalyticsPath + '/dataAnalyticsExtract_' + dataName + '_' + qualityFlag + '.csv'

