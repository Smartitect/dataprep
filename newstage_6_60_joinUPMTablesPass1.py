#%% [markdown]
# # Join UPM Tables Pass 1
# To join two tables:
# - Define left and right dataflows
# - Define left and right join keys
# NOTE - future enhancement would be to quarantine orphaned rows from left and right dataflows

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonDataFlowProcessingLoop import dataFlowProcessingLoop
from commonInventoryCreation import getColumnStats, getDataFlowStats
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage
from commonJoinHandling import joinTables

# Let's also set up global variables and common functions...
previousStageNumber = '50'
thisStageNumber = '60'

#%%
dataFlowInventoryAll = dataFlowProcessingLoop(previousStageNumber, thisStageNumber, 'A', 'JoinUPMTablesPass1', joinTables)

#%%
dataFlowInventoryAll