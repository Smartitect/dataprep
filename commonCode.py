# Import all of the libraries we need to use
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import datetime
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow

# Let's also set up global variables and common functions...
# NOTE - still to figure out how to do this from a single file and import it successfully.
#%%
# Path to the source data
dataPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

# A helper function to create full package path
def createFullPackagePath(packageName, stage, qualityFlag):
    thisStagePath = packagePath + '/' + packageName + '/' + stage

    if not os.path.isdir(thisStagePath):
        os.mkdir(thisStagePath)

    return thisStagePath + '/' + packageName + '_' + qualityFlag + packageFileSuffix

# A save package helper function
def savePackage(dataFlowToPackage, packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    dataFlowToPackage.save(fullPackagePath)
    return fullPackagePath

def saveColumnInventoryForTable(columnInventory, packageName, stage):
    thisStagePath = packagePath + '/' + packageName + '/' + stage

    if not os.path.isdir(thisStagePath):
        os.mkdir(thisStagePath)

    columnInventory.to_csv(thisStagePath + '/' + 'columnInventory_' + stage + '_Out.csv', index = None)

# An open package helper function
def openPackage(packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    dataFlow = Dataflow.open(fullPackagePath)
    return dataFlow

# A data profiling helper function to capture column metrics in a standard way
def getTableStats(dataProfile, dataName, stage):
    dataInventory = pd.DataFrame()
    # NOTE - there's got to be a more elegant way of doing this!
    columnNameList = [c.column_name for c in dataProfile.columns.values() if c.column_name]
    columnNameCol = pd.DataFrame({'Name':columnNameList})
    dataInventory = pd.concat([dataInventory, columnNameCol], axis=1)
    columnTypeList = [c.type for c in dataProfile.columns.values() if c.type]
    columnTypeCol = pd.DataFrame({'Type':columnTypeList})
    dataInventory = pd.concat([dataInventory, columnTypeCol], axis=1)
    columnMinList = [c.min for c in dataProfile.columns.values() if c.min]
    columnMinCol = pd.DataFrame({'Min':columnMinList})
    dataInventory = pd.concat([dataInventory, columnMinCol], axis=1)
    columnMaxList = [c.max for c in dataProfile.columns.values() if c.max]
    columnMaxCol = pd.DataFrame({'Max':columnMaxList})
    dataInventory = pd.concat([dataInventory, columnMaxCol], axis=1)
    columnRowCountList = [c.count for c in dataProfile.columns.values() if c.count]
    columnRowCountCol = pd.DataFrame({'RowCount':columnRowCountList})
    dataInventory = pd.concat([dataInventory, columnRowCountCol], axis=1)
    columnMissingCountList = [c.missing_count for c in dataProfile.columns.values() if c.missing_count]
    columnMissingCountCol = pd.DataFrame({'MissingCount':columnMissingCountList})
    dataInventory = pd.concat([dataInventory, columnMissingCountCol], axis=1)
    columnErrorCountList = [c.error_count for c in dataProfile.columns.values() if c.error_count]
    columnErrorCountCol = pd.DataFrame({'ErrorCount':columnErrorCountList})
    dataInventory = pd.concat([dataInventory, columnErrorCountCol], axis=1)
    columnEmptyCountList = [c.empty_count for c in dataProfile.columns.values() if c.empty_count]
    columnEmptyCountCol = pd.DataFrame({'EmptyCount':columnEmptyCountList})
    dataInventory = pd.concat([dataInventory, columnEmptyCountCol], axis=1)
    dataInventory.insert(0, 'DataName', dataName)
    dataInventory.insert(1, 'Stage', stage)
    dataInventory.insert(2, 'DateTime', datetime.datetime.now())
    print('{0}: column_name {1} type {2} min {3} max {4} count {5} missing_count {6} error_count {7} empty_count {8}'.format(dataName, len(columnNameList), len(columnTypeList), len(columnMinList), len(columnMaxList), len(columnRowCountList), len(columnMissingCountList), len(columnErrorCountList), len(columnEmptyCountList)))
    return dataInventory

# An open package helper function with full path as parameter
def openPackageFromFullPath(fullPath):
    dataFlow = Dataflow.open(fullPath)
    return dataFlow
