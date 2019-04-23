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

# Path to where we will store the data flow inventories
dataFlowInventoryPath = "./dataFlowInventory"

# Path to where we will store the column inventories
columnInventoryPath = "./columnInventory"

# A helper fucntion to consistently create the full path to the column inventory
def createFullColumnInventoryPath(stageNumber):
    if not os.path.isdir(columnInventoryPath):
        os.mkdir(columnInventoryPath)
    return columnInventoryPath + '/' + 'columnInventory_' + stageNumber + '.csv'

# Save the column inventory
def saveColumnInventory(columnInventoryAll, stageNumber):
    fullColumnInventoryPath = createFullColumnInventoryPath(stageNumber)
    columnInventoryAll.to_csv(fullColumnInventoryPath, index = None)
    return fullColumnInventoryPath

# A helper fucntion to consistently create the full path to the dataflow inventory
def createFullDataFlowInventoryPath(stageNumber):
    if not os.path.isdir(dataFlowInventoryPath):
        os.mkdir(dataFlowInventoryPath)
    return dataFlowInventoryPath + '/' + 'dataFlowInventory_' + stageNumber + '.csv'

# Save the dataflow inventory
def saveDataFlowInventory(dataFlowInventoryAll, stageNumber):
    fullDataFlowInventoryPath = createFullDataFlowInventoryPath(stageNumber)
    dataFlowInventoryAll.to_csv(fullDataFlowInventoryPath, index = None)
    return fullDataFlowInventoryPath

# A data profiling function to capture metrics for all columns in a data flow in a standard way
def getColumnStats(dataProfile, dataName, stageNumber, operatorToUse, operationFlag):
    
    # Set up the empty dataframe ready to store the results for each column in the dataFlow
    columnStats = pd.DataFrame(columns = [ \
        'DataName', \
        'Stage', \
        'OperatorToUse', \
        'OperationFlag', \
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
        
        # Check to see if this column has "value counts"
        if item.value_counts == None:
            valueCount = None
        else:
            valueCount = len(item.value_counts)

        # Append all of the statistics as a new row in the dataframe
        columnStats = columnStats.append({ \
            'DataName' : dataName, \
            'Stage' : stageNumber, \
            'OperatorToUse' : operatorToUse, \
            'OperationFlag' : operationFlag, \
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
    
    # Last step is to insert the date and time column as a single step
    columnStats.insert(2, 'DateTime', datetime.datetime.now())

    # Also return the number of columns
    numberOfColumns = len(dataProfile.columns.values())

    return columnStats

# A data profiling function to capture data flow level metrics in a standard way
def getDataFlowStats(dataFlow, dataProfile, dataName, stageNumber, operatorToUse, operationFlag):
        
    # Set up the empty dataframe ready to store the results
    dataFlowStats = pd.DataFrame(columns = [ \
    'DataName', \
    'Stage', \
    'OperatorToUse', \
    'OperationFlag', \
    'Rows', \
    'Columns'])

    # Build the stats
    rows = dataFlow.row_count
    columns = len(dataProfile.columns.keys())

    # Append all of the statistics as a new row in the dataframe
    dataFlowStats = dataFlowStats.append({ \
        'DataName' : dataName, \
        'Stage' : stageNumber, \
        'OperatorToUse' : operatorToUse, \
        'OperationFlag' : operationFlag, \
        'Rows' : rows, \
        'Columns' : columns }, ignore_index = True)

    # Last step is to insert the date and time column as a single step
    dataFlowStats.insert(2, 'DateTime', datetime.datetime.now())
    
    return dataFlowStats
