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

# Path to where we will store the data validation checks
dataValidationPath = "./dataValidation"

# A helper fucntion to consistently create the full path to the data validation folder
def createFullDataValidationPath(checkGroup, checkName):
    if not os.path.isdir(dataValidationPath):
        os.mkdir(dataValidationPath)
    return dataValidationPath + '/' + 'columnInventory_' + checkGroup + '_' + checkName + '.csv'

# Save the column inventory
def saveDataValidations(dataValidationAll, checkGroup, checkName):
    fullDataValidationPath = createFullDataValidationPath(checkGroup, checkName)
    dataValidationAll.to_csv(fullDataValidationPath, index = None)
    return fullDataValidationPath

# A function to capture metrics for all data validation checks in a standard way
def logDataValidationChecks(dataValidations, columnWithID, checkGroup, checkName):

    # NOTE - should put some kind of check in here to make sure all of the expected columns are there...
              
    # Last step is to insert the date and time column as a single step
    dataValidations.insert(2, 'DateTime', datetime.datetime.now())

    # Last step is to insert the date and time column as a single step
    dataValidations.insert(2, 'CheckGroup', checkGroup)

    # Last step is to insert the date and time column as a single step
    dataValidations.insert(2, 'CheckName', checkName)

    resultStats = dataValidations.groupby('Result').count()

    fullDataValidationPath = saveDataValidations(dataValidations, checkGroup, checkName)

    return fullDataValidationPath, resultStats

# This function performs row by row level checks on the defined column to flag nulls, empty and errors.
def insertBasicCheckColumnsIntoDataFlow(dataFlow, columnNameToCheck):

    # Check for nulls...
    nullCheckColumnName = columnNameToCheck + '_' + 'IsNull'
    dataFlow = dataFlow.add_column(new_column_name=nullCheckColumnName,
                           prior_column=columnNameToCheck,
                           expression=(dataFlow[columnNameToCheck].is_null()))

    # Check if it is empty...
    emptyCheckColumnName = columnNameToCheck + '_' + 'IsEmpty'
    dataFlow = dataFlow.add_column(new_column_name=emptyCheckColumnName,
                            prior_column=columnNameToCheck,
                            expression=(dataFlow[columnNameToCheck] == ''))

    # Check for errors...
    errorCheckColumnName = columnNameToCheck + '_' + 'HasErrors'
    dataFlow = dataFlow.add_column(new_column_name=errorCheckColumnName,
                           prior_column=columnNameToCheck,
                           expression=(dataFlow[columnNameToCheck].is_error()))

    print('{0} : inserted {1}, {2} and {3} colums'.format(columnNameToCheck, nullCheckColumnName, emptyCheckColumnName, errorCheckColumnName))

    return dataFlow, nullCheckColumnName, emptyCheckColumnName, errorCheckColumnName

# This function:
# - Consolidates the null, empty and error check boolean flags into pass / fail statements
# - Outputs a dataframe ready for more advanced validations to be performed against
def generateBasicValidationResults(dataFrame, columnWithID, checkColumn):
    
    def turnBooleanIntoResult(row, checkColumn, positiveResult = False):
        if row[checkColumn] is positiveResult:
            row['Result'] ='Pass'
            row['Comment'] = ''
        elif row[checkColumn] is not positiveResult:
            row['Result'] ='Fail'
            row['Comment'] = ''
        else:
            row['Result'] ='Error'
            row['Comment'] = 'Something went wrong'
        return row

    dataValidations = dataFrame[[columnWithID, checkColumn]].apply(lambda row: \
        turnBooleanIntoResult(row, checkColumn, False), axis=1)
    
    print('Procesed validation results for {0} column'.format(checkColumn))

    return dataValidations

def performBasicValidationsOnColumn(dataFlow, checkGroup, columnWithID, columnNameToCheck):

    dataFlow, nullCheckColumnName, \
        emptyCheckColumnName, errorCheckColumnName = \
            insertBasicCheckColumnsIntoDataFlow(dataFlow, columnNameToCheck)

    dataFrame = dataFlow.to_pandas_dataframe()
    print('Converted dataflow into a pandas dataframe')

    data = [[nullCheckColumnName, 'NullCheck'], [emptyCheckColumnName, 'EmptyCheck'], [errorCheckColumnName, 'ErrorCheck']]
    checkListDataFrame = pd.DataFrame(data, columns = ['ColumnName', 'CheckType'])

    for index, row in checkListDataFrame.iterrows():

        # Generate dataValidations - a data frame with just 4 columns : ID, check result (pass/fail/error), comment
        dataValidations = generateBasicValidationResults(dataFrame, columnWithID, row['ColumnName'])

        checkName = columnNameToCheck + '_' + row['CheckType']
        fullDataValidationPath, resultStats = logDataValidationChecks(\
            dataValidations[[columnWithID, 'Result', 'Comment']], \
            columnWithID, checkGroup, checkName)
        print('Saved results for {0} to path {1}'.format(checkName, fullDataValidationPath))
        print(resultStats)

    return dataFlow, dataFrame

