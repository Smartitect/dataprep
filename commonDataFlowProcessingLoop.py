# This loop will perform common processing on a set of dataFlows:
# - It will be driven by the "dataFlowController" file
# - For each row in that file it:
# - - Opens the dataFlow saved by the previsous stage;
# - - Reads the "operatorToUse" which is reference to the column in the "dataFlowController" file that provides input data to the processing - this can be either a boolean (Yes/No), a single item of data or a reference to a file which contains more complex data;
# - - Calls the "fucntionToCall" passing the "operatorToUse" and any other parameters passed through as **kwargs;
# - - Profiles the dataFlow and the individual columns in the data flow - saving the results to inventory files;
# - - Finally, it saves the new dataFlow so that it can be picked up at the next stage of the process.

# I found this web site useful for the use of **kwargs (multiple key word arguments):
# https://www.saltycrane.com/blog/2008/01/how-to-use-args-and-kwargs-in-python/

# I also found this reference useful for returning multiple values from a fucntion:
# https://www.mantidproject.org/Working_With_Functions:_Return_Values

# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import datetime
from datetime import datetime
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonPackageHandling import saveDataFlowPackage, openDataFlowPackage
from commonInventoryCreation import getColumnStats, saveColumnInventory, getDataFlowStats, saveDataFlowInventory


def dataFlowProcessingLoop (previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, functionToCall, **kwargs):
    
    # Load the dataFlow controller file
    dataFlows = dprep.read_csv('dataFlowController.csv').to_pandas_dataframe()

    # Set up empty dataframes that we will use to build up inventories at both dataFlow and column level
    dataFlowInventoryAll = pd.DataFrame()
    columnInventoryAll = pd.DataFrame()

    for index, row in dataFlows.iterrows():

        dataName = row["DataName"]
        operationFlag = row[operatorToUse]

        newDataFlow, columnInventory, dataFlowInventory = functionToCall(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag, **kwargs)

        if newDataFlow:

            # Capture the column inventory for the new dataflow
            columnInventoryAll = columnInventoryAll.append(columnInventory)
            print('{0}: appended {1} rows to column inventory'.format(dataName, len(columnInventory)))

            # Capture the data flow inventory for the new data flow
            dataFlowInventoryAll = dataFlowInventoryAll.append(dataFlowInventory)
            print('{0}: appended {1} rows to data flow inventory'.format(dataName, len(dataFlowInventory)))

    # Once we have processed all dataflows, we save the inventories away
    saveColumnInventory(columnInventoryAll, thisStageNumber)
    saveDataFlowInventory(dataFlowInventoryAll, thisStageNumber)

    return dataFlowInventoryAll