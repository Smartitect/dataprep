# All the common stuff we need to set up each notebook
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Package

# Let's also set up global variables
#%%
# Path to the source data
dataPath = "./data"

# Path to the location where the dataprep packags that are created
packagePath = "./packages"

# Name of package file
packageFileSuffix = "_package.dprep"

# Create a full path builder helper function for packages
def savePackage(dataFlowToPackage, packageName, stage, qualityFlag):
    dataFlowToPackage = dataFlowToPackage.set_name(packageName)
    packageToSave = dprep.Package(dataFlowToPackage)
    fullPackagePath = packagePath + '/' + packageName + '_' + stage + '_' + qualityFlag + packageFileSuffix
    packageToSave = packageToSave.save(fullPackagePath)