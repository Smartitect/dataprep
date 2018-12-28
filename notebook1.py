#%%
from commonCode.py import *

# Load in file names to be processed from the config.csv file
dataFiles = dprep.read_csv('dataFiles.csv').to_pandas_dataframe()

# Create a fully qualified path to the data files and append this to the dataFiles data frame
fullFilePaths = dataPath + '/' + dataFiles.FileName
fullFilePaths.name = "FullFilePath"
dataFiles = pd.concat([dataFiles, fullFilePaths], axis=1)

# now grab the number of headers in the first row of each file
expectedColumnCount = []
for index, row in dataFiles.iterrows():
    firstRow = open(row["FullFilePath"]).readline().strip()
    pattern = re.compile(',\w')
    patternCount = len(re.findall(pattern,firstRow))
    expectedColumnCount.append(patternCount + 1)
columnCount = pd.DataFrame({'ColumnCount':expectedColumnCount})
dataFiles = pd.concat([dataFiles, columnCount], axis=1)
dataFiles

#%% [markdown]
#---
## Stage 1 + 2 : Ingest and Manipulate Part 1
# Stepping through each file in the config.csv file to extract and do a basic clean up:
# - Load the CSV data;
# - Replace the custom string `<null>` representing a null and any other empty cells to a real `null`;
# - Remove the first row;
# - Quarantine rows (extract them and put them into a parallel data flow so that they can be fixed at a later stage) which have values in columns that are not listed in the header record;
# - Try to detect data types in each column using **column types builder**
# - Save the data flow that has been created for each file away so that it can be referenced and used later on

#%%
for index, row in dataFiles.iterrows():
    dataName = row["DataName"]
    fullFilePath = row["FullFilePath"]
    columnCount = row["ColumnCount"]
    packageName = row["PackageName"]
    # Load each file
    print('{0}: loading data from file path {1}'.format(dataName, fullFilePath))
    dataFlow = dprep.read_csv(fullFilePath)
    print('{0}: loaded {1} rows'.format(dataName, dataFlow.row_count))
    # Get a list of the columns
    dataFlowColumns = list(dataFlow.get_profile().columns.keys())
    # Now clean up the data in those columns
    print('{0}: cleaning up {1} columns (expected {2})'.format(dataName, len(dataFlowColumns), columnCount))
    # Replace any instances of the <null> string
    dataFlow = dataFlow.replace_na(dataFlowColumns, custom_na_list='<null>')
    # Remove the first row
    # NOTE : it would be good to add check to this to make sure it is the blank row we anticipate that begins `SCHEME=AR` 
    dataFlow = dataFlow.skip(1)
    print('{0}: removed first row, down to {1} rows'.format(dataName, dataFlow.row_count))
    # Quarantine rows which don't have the right columns
    quarantinedDataFlow = dataFlow.drop_nulls(dataFlowColumns[columnCount:])
    print('{0}: created quarantined data with {1} rows'.format(dataName, quarantinedDataFlow.row_count))
    # Filter out the quarantined rows from the main data set
    # NOTE : can't figure out a better way of doign this for now - see note below...
    for columnToCheck in dataFlowColumns[columnCount:]:
        # Don't know why line of code below doesn't work!
        # dataFlow = dataFlow.filter(dataFlow[columnToCheck] != '')
        dataFlow = dataFlow.assert_value(columnToCheck, value != '' , error_code='ShouldBeNone')
        dataFlow = dataFlow.filter(col(columnToCheck).is_error())
        print('{0}: filtered column {1}, row count now {2}'.format(dataName, columnToCheck, dataFlow.row_count))
    # Now drop the extra columns
    dataFlow = dataFlow.drop_columns(dataFlowColumns[columnCount:])
    print('{0}: dropped {1} unwanted columns'.format(dataName, len(dataFlowColumns[columnCount:])))
    # Detect and apply column types
    builder = dataFlow.builders.set_column_types()
    builder.learn()
    builder.ambiguous_date_conversions_keep_month_day()
    dataFlow = builder.to_dataflow()
    # Finally save the data flow so it can be used later
    print('{0}: saving package {1} to file {2}'.format(dataName, packageName, fullPackagePath))
    dataFlow = dataFlow.set_name(packageName)
    packageToSave = dprep.Package(dataFlow)
    packageToSave = packageToSave.save(fullPackagePath)
    print('{0}: saving package {1} to file {2}'.format(dataName, 'Q_' + packageName, fullPackagePath))
    dataFlow = dataFlow.set_name('Q_' + packageName)
    packageToSave = dprep.Package(dataFlow)
    packageToSave = packageToSave.save(fullPackagePath)

#%% [markdown]
# I'm struggling to find a method of filtering rows that have values in unanticipated columns - for example, the following does not work as I can't pass in the the `dataFlowColumns` list to the script block.
#```
# testdataFlow = dataFlow.new_script_filter("""
# def includerow(row, dataFlowColumns):
#    val = row[dataFlowColumns].isnull().any(index=None)
#    return
# """)
# ```

#%%
dataFlow.get_profile()

#%%
dataFlow.head(100)
