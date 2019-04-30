#%% [markdown]
# # Validation : UPM_FOLDER_UPMPERSON
# Runs a range of validations against the UPMFOLDER_UPMPERSON data flow
# Results are output in a standarised format for visuaisaiton / analyitcs / reporting

#%%
# Import all of the libraries we need to use...
import pandas as pd
import numpy as np
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
from commonValidationFunctions import logDataValidationChecks, performBasicValidationsOnColumn

# Set up global variables and common functions...
previousStageNumber = '60'
thisStageNumber = '62'
qualityFlag = 'A'

#%%
# Load the data flow that we are going to perform the validations on...
dataFlowToValidate = 'UPMFOLDER_UPMPERSON'

dataFlow, fullPackagePath = openDataFlowPackage(dataFlowToValidate, previousStageNumber, qualityFlag)
print('{0}: loaded package from path {1}'.format(dataFlowToValidate, fullPackagePath))

#%% [markdown]
# ## Validations : Date of Birth
# This section will deal with validations on Date of Birth

#%%
# Assign a common "checkGroup"
checkGroup = 'Date of Birth'

# Identify the column in which the foreign key will be provided for cross referencing data validation checks with core member data
columnWithID = 'FOLDERID'

# Identify the columns we are going to work with...
columnNameDOB = 'DOB'
columnNameDJC = 'DATEJOINEDCOMP'

#%% [markdown]
# ### First consolidate basic null, empty and data type error checks on Date of Birth and Date Joined Company

#%%
# First Date of Birth
dataflow, dataFrame = performBasicValidationsOnColumn(dataFlow, checkGroup, columnWithID, columnNameDOB)

#%%
# Second Date Joined Company
dataflow, dataFrame = performBasicValidationsOnColumn(dataFlow, checkGroup, columnWithID, columnNameDJC)

#%% [markdown]
# ### Now run a member by member check regarding Date of Birth being earlier than Date Joined Company

#%%
def checkDateOfBirthLessThanDateJoinedCompany(row, columnNameDOB, columnNameDJC):
        if row[columnNameDOB] >= row[columnNameDJC]:
            row['Result'] = 'Fail'
            row['Comment'] = 'DOB is greater than DJC'
        elif row[columnNameDOB] < row[columnNameDJC]:
            row['Result'] = 'Pass'
            row['Comment'] = ''
        else:
            row['Result'] = 'Error'
            row['Comment'] = 'Something went wrong'
        return row

dataValidations = dataFrame[[columnWithID, columnNameDOB, columnNameDJC]].apply(lambda row: checkDateOfBirthLessThanDateJoinedCompany(row, columnNameDOB, columnNameDJC), axis=1)

#%%
# Now save the results
logDataValidationChecks(dataValidations, columnWithID, checkGroup, 'DOB_LT_DJC')