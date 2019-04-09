# Import all of the libraries we need to use
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
import os as os
import re as re
import collections
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
    return packagePath + '/' + packageName + '_' + stage + '_' + qualityFlag + packageFileSuffix

# A save package helper function
def savePackage(dataFlowToPackage, packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    packageToSave = dataFlowToPackage.save(fullPackagePath)
    return fullPackagePath

# An open package helper function
def openPackage(packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    dataFlow = Dataflow.open(fullPackagePath)
    return dataFlow