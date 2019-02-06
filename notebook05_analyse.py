#%% [markdown]
# # Stage 5 - Analyse
# This is where we want to apply all of the generic data quality checks

# ## Common
# So this is where we are trying to do all the common stuff to ingest all of the files.  Key is recognition that there are common patterns we can exploit across the files.
# NOTE - still to figure out how to do this from a single file and import it successfully.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
import os as os
import re as re
import collections
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Package

# Let's also set up global variables and common functions...

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
    dataFlowToPackage = dataFlowToPackage.set_name(packageName)
    packageToSave = dprep.Package(dataFlowToPackage)
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    packageToSave = packageToSave.save(fullPackagePath)
    return fullPackagePath

# An open package helper function
def openPackage(packageName, stage, qualityFlag):
    fullPackagePath = createFullPackagePath(packageName, stage, qualityFlag)
    packageToOpen = Package.open(fullPackagePath)
    dataFlow = packageToOpen[packageName]
    return dataFlow

#%% [markdown]
# ## Open CANONICAL data flow
# NOTE - Should be picking up canonical data flow from stage 4, but we don't have that defined yet.
# So will pick up pick up the JOINED the data flow from stage 3 for now and refactor later on...

#%%
canonicalDataFlow = openPackage('JOINED', '3', 'A')

#%% [markdown]
# ## Date Checks
# NOTE - it would be great if you could do this using an `dataflow.assert_value` call but it appears you can can't reference other columns as part of this.
#%%
builder = canonicalDataFlow.builders.split_column_by_example(source_column="PEOPLE_DOB")
builder.add_example(example=('1913-04-03', ['1913', '04', '03']))
canonicalDataFlow = builder.to_dataflow()
#%%
canonicalDataFlow = canonicalDataFlow.rename_columns(column_pairs={
    "PEOPLE_DOB_1": "PEOPLE_DOB_YEAR",
    "PEOPLE_DOB_2": "PEOPLE_DOB_MONTH",
    "PEOPLE_DOB_3": "PEOPLE_DOB_DAY"
})   

peopleDataFlow = canonicalDataFlow.to_long(columns=['PEOPLE_DOB_YEAR', 'PEOPLE_DOB_MONTH', 'PEOPLE_DOB_DAY'])

#%%
from azureml.dataprep import ReplacementsValue
replacements = [
    ReplacementsValue('Mrs', 'F'),
    ReplacementsValue('Mr', 'M'),
    ReplacementsValue('Miss', 'F'),
    ReplacementsValue('Ms', 'F'),
    ReplacementsValue('Dr', None),
    ReplacementsValue('Rev', None),
    ReplacementsValue('Sister', 'F'),
    ReplacementsValue('Sir', 'M'),
    ReplacementsValue('Prof', None),
    ReplacementsValue('Doct', None),
    ReplacementsValue('Fr', None),
    ReplacementsValue('Dame', 'F')
    ]
canonicalDataFlow = canonicalDataFlow.map_column(column='PEOPLE_TITLE_grouped', 
                                new_column_id='PEOPLE_TITLE_SEX',
                                replacements=replacements)
#%%
canonicalDataFlow = canonicalDataFlow.add_column(new_column_name='Test2_TitleSex',
                           prior_column='MEMBERS_DJS',
                           expression=(canonicalDataFlow['PEOPLE_TITLE_SEX'] == canonicalDataFlow['PEOPLE_SEX']))
#%%
canonicalDataFlow = canonicalDataFlow.add_column(new_column_name='Test3_DJSgtDJC',
                           prior_column='Test2_TitleSex',
                           expression=(canonicalDataFlow['MEMBERS_DJS'] > canonicalDataFlow['MEMBERS_DJC']))
#%%
canonicalDataFlow = canonicalDataFlow.add_column(new_column_name='Test4_DJSgtDOB',
                           prior_column='Test3_DJSgtDJC',
                           expression=(canonicalDataFlow['MEMBERS_DJS'] > canonicalDataFlow['PEOPLE_DOB']))
#%%
canonicalDataFlow = canonicalDataFlow.add_column(new_column_name='Test5_MissingRetirementDate',
                           prior_column='Test4_DJSgtDOB',
                           expression=(canonicalDataFlow['PEOPLE_MINRETIREMENTDATE'] != '' ))
#%%
canonicalDataFlow = canonicalDataFlow.add_column(new_column_name='TestFinal_RollUpAllTests',
                           prior_column='Test5_MissingRetirementDate',
                           expression=(canonicalDataFlow['Test2_TitleSex'] & 
                           canonicalDataFlow['Test3_DJSgtDJC'] &
                           canonicalDataFlow['Test4_DJSgtDOB'] &
                           canonicalDataFlow['Test5_MissingRetirementDate']))
#%%
#canonicalDataFlow = canonicalDataFlow.new_script_column(new_column_name='Test3', insert_after='Test2', script="""
#def newvalue(row):
#    if row['MEMBERS_DJS'] == None or row['MEMBERS_DJC'] == '':
#        return None
#    elif row['MEMBERS_DJS'] < row['MEMBERS_DJC']:
#        return "DJS gt DJC"
#    else:
#        return "DJC lte DJS"
#""")
#%%
profile = canonicalDataFlow.get_profile()

#%% [markdown]
# ## Reporting on findings
# ### TEST 1 : check distrbution of Date of Birth for anomalies
#%%
profile.columns['PEOPLE_DOB_YEAR'].value_counts
#%%
profile.columns['PEOPLE_DOB_MONTH'].value_counts
#%%
profile.columns['PEOPLE_DOB_DAY'].value_counts
#%% [markdown]
# ### TEST 2 : Check that sex is consistent with title
#%%
profile.columns['Test2_TitleSex'].value_counts
#%% [markdown]
# ### TEST 3 : check Date Joined Scheme (DJS) is greater than Date Joined Company (DHC)
#%%
profile.columns['Test3_DJSgtDJC'].value_counts
#%% [markdown]
# ### TEST 4 : check Date Joined Scheme (DJS) is greater than Date of Birth (DOB)
#%%
profile.columns['Test4_DJSgtDOB'].value_counts
#%% [markdown]
# ### TEST 5 : Flag rows with missing retirement dates
#%%
profile.columns['Test5_MissingRetirementDate'].value_counts
#%% [markdown]
# Looking deeper into the above:
#%%
profile.columns['PEOPLE_MINRETIREMENTDATE'].value_counts
#%% [markdown]
# ### FINAL TEST : Flag rows that fail all tests above:
#%%
profile.columns['TestFinal_RollUpAllTests'].value_counts
#%%
fullPackagePath = savePackage(canonicalDataFlow, 'ANALYSED', '5', 'A')
print('Saved package to file {0}'.format(fullPackagePath))

#%%
dflow_write = canonicalDataFlow.write_to_csv(directory_path=dprep.LocalFileOutput('./output/'))
dflow_write.run_local()