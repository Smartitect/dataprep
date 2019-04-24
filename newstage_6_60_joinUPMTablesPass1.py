#%% [markdown]
# # Stage : Create UPM dataflows
# Uses a config file to drive which UPM dataflows we should create
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
from commonJoinHandling import joinTables

# Let's also set up global variables and common functions...
previousStageNumber = '50'
thisStageNumber = '60'

#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'JoinUPMTablesPass1', joinTables)

#%%
dataFlowInventoryAll