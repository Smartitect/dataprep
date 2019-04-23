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

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

# A helper function to create full package path
def createFullPackagePath(dataName, stage, qualityFlag):
    thisStagePath = packagePath + '/' + dataName + '/' + stage

    if not os.path.isdir(packagePath):
        os.mkdir(packagePath)

    if not os.path.isdir(packagePath + '/' + dataName):
        os.mkdir(packagePath + '/' + dataName)

    if not os.path.isdir(thisStagePath):
        os.mkdir(thisStagePath)

    return thisStagePath + '/' + dataName + '_' + qualityFlag + packageFileSuffix

def createNewPackageDirectory(newdataName):
    if os.path.isdir(packagePath + '/' + newdataName):
        shutil.rmtree(packagePath + '/' + newdataName)

    os.mkdir(packagePath + '/' + newdataName)

# A save package helper function
def saveDataFlowPackage(dataFlowToPackage, dataName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(dataName, stage, qualityFlag)
    dataFlowToPackage.save(fullPackagePath)
    savePendletonPackage(fullPackagePath, dataName, stage, qualityFlag)
    return fullPackagePath

def savePendletonPackage(path, dataName, stage, qualityFlag):
    pendeltonFolderPath = packagePath + '/' + dataName + '/' + stage + '/pendleton'
    pendletonFilePath = pendeltonFolderPath + '/' + dataName + '_' + qualityFlag + packageFileSuffix

    if os.path.isdir(pendeltonFolderPath):
        shutil.rmtree(pendeltonFolderPath)

    if not os.path.isdir(pendeltonFolderPath):
        os.mkdir(pendeltonFolderPath)

    with open(path, 'r+') as f:
        f.seek(1,0)
        a = f.read()
        with open(pendletonFilePath, 'x') as f:
            f.write('{"schemaVersion": 63,"id": "4d5dccfb-2c5f-488d-9b7a-be4071db9cac","activities": [{"id": "3be82a4a-d2d0-47b0-929d-d9d77215ac54","name": "'+ dataName + '",' + a + '],"runConfigurations": []}')

# An open package helper function
def openDataFlowPackage(dataName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(dataName, stage, qualityFlag)
    if os.path.isfile(fullPackagePath):
        dataFlow = Dataflow.open(fullPackagePath)
        return dataFlow, fullPackagePath
    else:
        return None, fullPackagePath

# An open package helper function with full path as parameter
def openDataFlowPackageFromFullPath(fullPath):
    dataFlow = Dataflow.open(fullPath)
    return dataFlow