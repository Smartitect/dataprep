# Overview

Proof of concept to assess how dataprep could be leveraged to support general data wrangling of pension scheme data.

# Installation

Download [Python runtime](https://www.python.org/downloads/windows/) - prefer the 64bit version & add to Path.

Via VSCode Terminal:

Install Jupyter: `python -m pip install jupyter`
Install Pandas: `python -m pip install pandas`
Install DataPrep SDK `python -m pip install azureml.dataprep`
Install certifi `python -m pip install certifi`

# Meta Process

Plan is to adopt a structured approach using the notebook:
1. Ingest - load each of the raw files from source repository and apply manipulation that is common to all files - such as:
    - Removing redundant leading rows;
    - Make sure empty cells are set to "None" correctly;
    - Automatically detect and apply the right primary data types: ie string, datetime, number or boolean.
2. Manipulate - apply more sophisticated manipulation tailored to each specific data file loaded based on profiling the data to address any anomalies;
3. Join - join the data sets together;
4. Map to Canonical Form - with all of the data assembled and joined, we can now map to a canonical form and apply generic data quality checks and analysis
5. Analyse - apply the more sophisticated data quality and data reconciliation checks;
5. Publish - publish the results:
    - Publish the output canonical data set, ready for import into operational platform;
    - Publish an audit report that includes all of the reconciliation checks;
    - Export the quarantied records that need to fixed (if any);
    - Publish a data quality report.

The concept of quarantining data at each stage is adopted to remove data data that does not meet minimum data quality requirements from the core workflow.  But in each case these quarantined data sets are saved to a branched data flow to create an audit trail and on the basis that they may be able to be fixed through some more advanced processing logic and replayed through the process at a later stage.

# Data quality scenarios:

| Requirement | Achieved? | Notes |
| --- | --- | --- |
| Dealing with poor quality raw data (e.g. commas in address column skewing CSV import) | Partial | Offending rows pushed into quaratine.  Not figured out how to address this yet. |
| Intelligent mapping to "canonical form"  | No | See observation  |
| TEST 1 : Spotting anomalies in the data (e.g. date of birth defaults used "1/1/1971") | Yes | Used split_column_by_example. |
| TEST 2 : Gender is not Male or Female / Incorrect Gender for members title (see below) | Yes | Used map_column. |
| Missing Addresses / Check for commas in members address / Missing postcodes | Partial | Cleaned up addresses.|
| TEST 3 : Date Joined Company is after Date Joined Scheme | Yes | Used simple add_colum with expression. |
| TEST 4 : Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth | Yes | Used simple add_colum with expression. |
| TEST 5 : Missing Scheme Retirement Date  | Yes | Used simple add_colum with expression. |
| TEST 6 Missing NI Number | Yes |  |
| TEST 7 Invalid NI Number | Yes |  |
| TEST 8 Temporary NI Number | Yes |  |
| Members annual pension should total sum of pension elements | No |  |
| Members annual pension not divisible by 12 | No |   |
| Repeating a recipe / looping to apply particular operations | Yes | See the stage 1 notebook, able to cycle through all files to apply common rules and save the "recipe" as a packaged up data flow that can be consumed downstream.|
| Using regular expressions/creating custom data expressions (see below) | No |   |
| Using union to join datasets | Partial | Seems to be working, but keen to do some analysis to see what it does when no matches are found on join.|
| Best practices for naming conventions | Partial | Through several iterations, have started to apply a logical meta model.|
| Interacting dynamically with the graphical representation to see the subset of data either passing or failing tests. | No |   |

## Observations so far
### 1 - Importing data from file
Struggling to use auto_read_file successfully to read in these files : losing the column names.  It would also be good to get guidance on how to get the options working, for example:
```
memberData = dprep.read_csv(folderPath, skip_rows=1, inference_arguments=dprep.InferenceArguments(day_first=False))
```
Unfortunately **skip_rows** leads to the header row being dropped and then the first row being promoted to a header row.
There is a **skip_mode** attirbute for **read_csv**, but struggling to find documentation for this.

Ended up reading in the file using a basic form of **read_csv** and performing the other steps at a later stage - eg dropping leading row / detecting data types.

### 2 - Filtering rows
I'm struggling to find a more elegant method of filtering rows that have values in unanticipated columns - for example, the following does not work as I can't pass in the the `dataFlowColumns` list to the script block.
```
testdataFlow = dataFlow.new_script_filter("""
def includerow(row, dataFlowColumns):
    val = row[dataFlowColumns].isnull().any(index=None)
    return
""")
```

Opted to using a weird mix of techniques to get what I needed:
- Used **dataFlow.drop_nulls** to filter down to the rows that I wanted to quarantine;
- Used **dataFlow.assert_value** and then a **dataFlow.filter(col(columnToCheck).is_error())** to filter out these rows.

There is a real risk here that there will be a logical mismatch between these two techniques such that rows are duplicated or missed altogether.  Hence thew following idea...

## 3 - Branched filtering
It would be great to send the rows are are filtered to a new data flow to make it more elegant to quarantine rows.
This would avoid the need to write two sets of statements : first set to first set to filter out offending rows to create the clean "A" class  dataflow and then a second set of statements to capture the offending rows into a quarantined "B" class dataflow for seperate cleaning / fixing (with the iea of pushing them back through the "A" class process once this has been achived).

## 4 - Fuzzy grouping not quite doing the job?
Surprised that fuzzy grouping didn't do a better job with both the MSTA and TITLE columns of data.  Are there other settings I could experiment, or are there ways to train the model over time based on our specific domain?

## 5 - Join - not detecting / suggesting join, also better documentation and perhaps control over how the "join" function is actually working?
Use of the join function is not auotmatically detecting / suggesting the join despite logical steps being taken to set up the two dataflows to enable this.
Also it would be good to understand if the join fucntion is performing an inner or outer join?  Can we control this?
For example, it would have be good to only join records from MEMBERS where there are matching records from PEOPLE, given that PEOPLE is the core record, and we have quarantined some of the original PEOPLE records.  So logically, there should be "orphaned" MEMBERS records.

## 6 - Statistics from the join process?
It would be good to get some feedback from the join process in terms of what it has been able to achieve in creating a successful join.

## 7 - Canonical mapping
No functionality exists to automate this task.  Could machine learning be used to analyse the target canonical form and suggest how the source could be mapped?  This would be really useful for trying to re-train existing models for example, as well as our specific use case here.

## 8 - Use of assert
This would be a killer capability if you could:
- Assert based on matching to a regular expressions (this is supported elsewhere natively such as filtering columns, could it be added here?);
- Assert based on comparing two columns in the data set (again, this is allowed in other functions such as new , so could it be added here?);
The net result of the above, is that you would not need to add extra data quality columns.  Instead, you could build up layers of meta data and then write some simple filters based on that meta data to filter out the rows that have assert related errors against them.

## 9 - More sophisticated filtering based on assert
Some kind of more sophisticated report that would generate a series of data flows from a source data flow that has had a series of **assert_value** statements applied to it?
This would be a great way of building up layers of errors based on applying assertions and then generating some kind of data structures at the end that be:
- Fed into downstream processing
- Have more sophisticated logic applied to them in order to clean them up
- Used to generate detailed data quality reports

---
# More detailed requirements...
## Validation agasint regular expressions
UK Postcode: to be applied to the `PEOPLE.POSTCODE` column:

```regexp
([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})
```

From: [stackoverflow](https://stackoverflow.com/questions/164979/uk-postcode-regex-comprehensive)

UK National Insurance Number: to be applied to the `PEOPLE.NINO` column:

```regexp
^([a-zA-Z]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([a-zA-Z]){1}?$
```

From: [stackoverflow](https://stackoverflow.com/questions/10204378/regular-expression-to-validate-uk-national-insurance-number)

## Validation using a look-up table

Show validation of the `PEOPLE.TITLE` column against a list based type (`SEX` for data quality check below):

|    TITLE   |     SEX       |
|------------|---------------|
|    Mrs     |    Female     |
|    Mr      |    Male       |
|    Miss    |    Female     |
|    Ms      |    Female     |
|    Dr      |    Unknown    |
|    Rev     |    Unknown    |
|    Sir     |    Male       |
|    Dame    |    Female     |


Table data is linked by `KeyObjectIDs` i.e. `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = SERVICE.MEMNO` etc.

Create a recipe that returns members (i.e. `Surname`, `NINO` ) who have a record present in the `EXITDEFERREDS` dataset with no corresponding entry in `STATHIS` where the `SSTA = 15`. The join between `EXITDEFERREDS` and `STATHIS` will need to incorporate the `MEMBERS & PEOPLE` datasets as these contains the link between the other two tables (For reference, `MEMNO` which is the "Common ID" between the three tables & `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = EXITDEFERREDS.MEMNO` etc.) This should return a subset of members without the correct entry in the `STATHIS` table showing us where Status History issues exist within the data.

This recipe should be easily adaptable to be used against the `EXITREFUND` and `EXITRETIREMENT` datasets to be used with different `SSTA` values. These `SSTA` values are contained within the `LOOKUPS` dataset but are not easily identifiable by a specific `KeyObjectID` or relevant column header.  Hence we have specified the value 15 for deferreds to make life easier for the recipe above. It would be interesting to see if there is a way to link or define a subset of the `LOOKUPS` table or perform matching via machine learning.

If Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth:
- New calculated column that flags when `PEOPLE.DOB` is greater than `MEMBERS.DJS` (see dataset join above)
- Also, calculate number of days between `DOB` and `DJS` (where negative numbers would allow us to spot other potential issues)

# Installation Instructions

Install a 64bit version of Python from https://www.python.org/downloads/windows/

Install VS Code and the Python Extension https://marketplace.visualstudio.com/items?itemName=ms-python.python

Install the DataPrep SDK via `pip install --upgrade azureml-dataprep`

Install the Jupyter notebooks extension for VS Code.

Synch the GitHub repository locally.

Drop the data files into a `./data` folder in the root location where all of the python files are located.