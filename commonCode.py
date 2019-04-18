# Import all of the libraries we need to use
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import datetime
import shutil
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

    if not os.path.isdir(packagePath):
        os.mkdir(packagePath)

    if not os.path.isdir(packagePath + '/' + packageName):
        os.mkdir(packagePath + '/' + packageName)

    if not os.path.isdir(thisStagePath):
        os.mkdir(thisStagePath)

    return thisStagePath + '/' + packageName + '_' + qualityFlag + packageFileSuffix

# A save package helper function
def savePackage(dataFlowToPackage, packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    dataFlowToPackage.save(fullPackagePath)
    savePendletonPackage(fullPackagePath, packageName, stage, qualityFlag)
    return fullPackagePath

def savePendletonPackage(path, packageName, stage, qualityFlag):
    pendeltonFolderPath = packagePath + '/' + packageName + '/' + stage + '/pendleton'
    pendletonFilePath = pendeltonFolderPath + '/' + packageName + '_' + qualityFlag + packageFileSuffix

    if os.path.isdir(pendeltonFolderPath):
        shutil.rmtree(pendeltonFolderPath)

    if not os.path.isdir(pendeltonFolderPath):
        os.mkdir(pendeltonFolderPath)

    with open(path, 'r+') as f:
        f.seek(1,0)
        a = f.read()
        with open(pendletonFilePath, 'x') as f:
            f.write('{"schemaVersion": 63,"id": "4d5dccfb-2c5f-488d-9b7a-be4071db9cac","activities": [{"id": "3be82a4a-d2d0-47b0-929d-d9d77215ac54","name": "'+ packageName + '",' + a + '],"runConfigurations": []}')


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
        
    for item in dataProfile.columns.values():
        
        if item.value_counts == None:
            valueCount = None
        else:
            valueCount = len(item.value_counts)

        columnStats = columnStats.append({'DataName' : dataName, \
        'Stage' : stage, \
        'ColumnName' : item.column_name, \
        'Type' : item.type, \
        'Min' : str(item.min), \
        'Max' : str(item.max), \
        'RowCount' : item.count, \
        'MissingCount' : item.missing_count, \
        'NotMissingCount' : item.not_missing_count, \
        'ErrorCount' : item.error_count, \
        'EmptyCount' : item.empty_count, \
        'Mean' : item.mean, \
        'ValueCount' : valueCount}, ignore_index = True)
    
    columnStats.insert(2, 'DateTime', datetime.datetime.now())
    
    return columnStats

# An open package helper function with full path as parameter
def openPackageFromFullPath(fullPath):
    dataFlow = Dataflow.open(fullPath)
    return dataFlow

def saveDataFileInventory(dataFiles, stageNumber, nextStageNumber):
    dataFiles.to_csv('dataFileInventory_' + stageNumber + '_Out.csv', index = None)
    dataFiles.to_csv('dataFileInventory_' + nextStageNumber + '_In.csv', index = None)

def gatherStartStageStats(stageNumber, dataFiles, rowCountStartList, columnCountStartList):
    rowCountStartCol = pd.DataFrame({'RowCountStartStage' + stageNumber:rowCountStartList})
    dataFiles = pd.concat([dataFiles, rowCountStartCol], axis=1)

    columnCountCol = pd.DataFrame({'ColumnCountStartStage'  + stageNumber:columnCountStartList})
    dataFiles = pd.concat([dataFiles, columnCountCol], axis=1)
    return dataFiles

def gatherEndStageStats(stageNumber, dataFiles, rowCountEndList, columnCountEndList, packageNameList):
    rowCountStartCol = pd.DataFrame({'RowCountEndStage' + stageNumber:rowCountEndList})
    dataFiles = pd.concat([dataFiles, rowCountStartCol], axis=1)

    columnCountCol = pd.DataFrame({'ColumnCountEndStage'  + stageNumber:columnCountEndList})
    dataFiles = pd.concat([dataFiles, columnCountCol], axis=1)

    packageNameCol = pd.DataFrame({'PackageNameStage'  + stageNumber:packageNameList})
    dataFiles = pd.concat([dataFiles, packageNameCol], axis=1)
    return dataFiles