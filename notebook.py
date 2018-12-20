#%% [markdown]
## Introduction

#[Hymans Robertson](https://hymans.co.uk) are a 100-year old company providing independent pensions, investments, benefits & risk consulting services, data & tech solutions to employers, trustees & financial services institutions, regulated by the Financial Conduct Authority in the UK.

#Since 2013, [endjin](https://endjin.com) have been [helping Hymans transform](https://endjin.com/industries/financial-services#hymans-robertson); initially by helping them migrate to Azure (and overcoming regulatory hurdles as they were the first UK FS customer to put PII data into Azure), next by helping to create an innovation process to enable them to start investing in product R&D to help the firm to digitally transform its service offerings.

### Background

#This experiment focuses on the "TPA (Third Party Administration) Installations" process. This occurs whenever we win a new a (pension) scheme and involves the transfer of data from the outgoing (or "ceding") administrator to Hymans Robertson.

#The process currently takes *8 weeks end-to-end*. It is a very serial process - i.e. Hymans can't get a detailed handle on data quality until 6-weeks into the 8-week process.

#The industry is skewed such that we run this process at a loss on the premise of making a return over the long term by administering the (pension) scheme.

#The process also acts as a bottleneck. Hymans can only support 10 to 12 installations per annum at full throttle. The company would love to be able to do more and therefore win more clients: the market is "coming to us" at the moment given the poor performance of other administrators and the strategic move from others to withdraw from the market (due to regulatory disruption).

#Poor data quality is rife in the industry. The party producing the data extract is doing so as part of project shut down after a commercial loss. This often means that a "quick and dirty" approach is taken, with no motivation to improve. So Hymans need to be able to work with ambiguity / gaps in the data, without seeking remedy at the source.

### Vision

#If Hymans can crack this problem it could be transformational for the company. Hymans could:

#- win more business (grow revenue) and make that business more profitable (increase profit margins) - in an industry where domination is becoming increasingly important, so there are significant "gold bars" downstream if we can increase market share
#- leverage the capability wider (new revenue streams) - e.g. offering data cleanse services to our competitors, or plugging this capability into wider services that involve data cleansing for administration data such as "buy-outs".
#- leverage the underlying technology and tools in other areas where we are handling similar data such as Club Vita.

### Design Principles

#1. The overarching requirement is that **Hymans must not loose any transactions as part of this process** - if we do, people won't get paid their pensions!  So the ability to reconcile raw data received from the ceding administrator with the final data set we load into our system is important.  This includes simple checks like transaction counts but also more complex checks such as summing up financial amounts to check they balance "to the penny".
#2. We get data from a number of different organisations using a relatively small set of off the shelf administration platforms.  Therefore the data we receive is broadly in one of a small number of recognisable formats.  However this varies - based on the version of the underlying administration platform AND based on the way it has been configured set up for that administrator AND based on the way it has been configured for that particular scheme. But we anticipate that we can build up a set of **reusable "recipes"** for each of these cases that can be tweaked quickly based on these variations.
#3. The current process takes about **8 weeks**, we'd love to transform that - reduce it to days rather than weeks, make it profitable in its own right, use some of the savings to put more effort into data quality with massive downstream benefits as we move increasingly to on-line administration - i.e. pension scheme members access services over web rather than phone / letter / email.
#4. The technical rigour around the production of data files can be variable, leading to us receiving **poor quality data** (e.g. unquoted commas in a CSV file), so we need to be able cope with this without having to do manipulation manually or outside the platform.
#5. Ability to **define canonical forms** on which to map data so that we can make downstream tasks common.
#6. Defining a **"meta process"** so that we can logically separate the different stages in the process: e.g. ingestion -> data integrity checks -> map to canonical form -> data type checks -> more complex data quality logic.
#7. Ability to orchestrate all of the above as part of a **modern, open cloud-based architecture** (i.e. data encrypted by default using BYOK, orchestrated by Azure Data Factory, serverless or pay per transaction).
#8. Further to the point above, the ability to **perform data quality checks at differing levels of complexity**, ranging simple data type checks (e.g. valid Postcode/ZIP code) to logic on a single transaction (e.g. date of birth earlier than date joined scheme) through to more complex checks that involve aggregation across multiple transactions/rows/members.
#9. In parallel Hymans have been working with Azure Cognitive Services to strip data out of scanned documents and handwritten notes (the unstructured data that accompanies the structured data we are focusing on here). Ultimately, we would like to do "fuzzy matching" to strip data out of these documents to enable us to plug gaps / improve data quality in the structured data. One broader goal to bear in mind at this point.

### Example Scenarios

#- Dealing with poor quality raw data (e.g. commas in address column skewing CSV import)
#- Intelligent mapping to "canonical form"
#- Spotting anomalies in the data (e.g. date of birth defaults used "1/1/1971")
#- Gender is not Male or Female / Incorrect Gender for members title
#- Missing Invalid or Temporary NI Number
#- Missing Addresses / Check for commas in members address / Missing postcodes
#- Date Joined Company is after Date Joined Scheme
#- Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth
#- Missing Scheme Retirement Date

### Scenarios joining tables, calculated basis, etc.

#- Members annual pension should total sum of pension elements
#- Members annual pension not divisible by 12

### Initial questions

#- Repeating a recipe/looping
#- Using regular expressions/creating custom data expressions
#- Using union to join datasets
#- Best practices for naming conventions
#- Interacting dynamically with the graphical representation to see the subset of data either passing or failing tests?

### Data Integrity, Validation Rules & Data Types

#NOTE: Table data is linked by `KeyObjectIDs` i.e. `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = SERVICE.MEMNO` etc.

#UK Postcode: to be applied to the `PEOPLE.POSTCODE` column:

#```regexp
#([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})
#```

#From: [stackoverflow](https://stackoverflow.com/questions/164979/uk-postcode-regex-comprehensive)

#UK National Insurance Number: to be applied to the `PEOPLE.NINO` column:

#```regexp
#^([a-zA-Z]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([a-zA-Z]){1}?$
#```

#From: [stackoverflow](https://stackoverflow.com/questions/10204378/regular-expression-to-validate-uk-national-insurance-number)

#Show validation of the `PEOPLE.TITLE` column against a list based type (`SEX` for data quality check below):

#|    TITLE   |     SEX       |
#|------------|---------------|
#|    Mrs     |    Female     |
#|    Mr      |    Male       |
#|    Miss    |    Female     |
#|    Ms      |    Female     |
#|    Dr      |    Unknown    |
#|    Rev     |    Unknown    |
#|    Sir     |    Male       |
#|    Dame    |    Female     |

#Create a recipe that returns members (i.e. `Surname`, `NINO` ) who have a record present in the `EXITDEFERREDS` dataset with no corresponding entry in `STATHIS` where the `SSTA = 15`. The join between `EXITDEFERREDS` and `STATHIS` will need to incorporate the `MEMBERS & PEOPLE` datasets as these contains the link between the other two tables (For reference, `MEMNO` which is the "Common ID" between the three tables & `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = EXITDEFERREDS.MEMNO` etc.) This should return a subset of members without the correct entry in the `STATHIS` table showing us where Status History issues exist within the data.

#This recipe should be easily adaptable to be used against the `EXITREFUND` and `EXITRETIREMENT` datasets to be used with different `SSTA` values. These `SSTA` values are contained within the `LOOKUPS` dataset but are not easily identifiable by a specific `KeyObjectID` or relevant column header.  Hence we have specified the value 15 for deferreds to make life easier for the recipe above. It would be interesting to see if there is a way to link or define a subset of the `LOOKUPS` table or perform matching via machine learning.

#If Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth:

#- New calculated column that flags when `PEOPLE.DOB` is greater than `MEMBERS.DJS` (see dataset join above)
#- Also, calculate number of days between `DOB` and `DJS` (where negative numbers would allow us to spot other potential issues)

### Installation Instructions

#Install a 64bit version of Python from https://www.python.org/downloads/windows/

#Install VS Code and the Python Extension https://marketplace.visualstudio.com/items?itemName=ms-python.python

#Install the DataPrep SDK via `pip install --upgrade azureml-dataprep`
#%% [markdown]
# Let's start by importing the data prep SDK and pandas library:
#%%
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
from azureml.dataprep import value

#%% [markdown]
#---
## Stage 1 : Read Data
### 1.1 Ingest PEOPLE
# Let's take a look at the **PEOPLE.csv** file. We'll import it using the `auto_read_file` (new name for `smart_read_file` if we're not mistaken) function. This will automatically detect the file type and how to parse it. If we're lucky, it will detect the types of each column and apply any corresponding type transformations.

#%%
folderPath = './data/people.csv'
peopleData = dprep.auto_read_file(folderPath)
peopleData.head(5)

#%% [markdown]
### 1.2 Profile PEOPLE
# Now let's inspect the columns detected on this file:
#%%
peopleData.get_profile()

#%% [markdown]
# We can immediately see that we have some problems with our data. Our first row isn't helpful. We have the new line code `\0d0a` in numerous columns.
# We also have extra columns defined in our dataset that we aren't expecting. This is possibly because we have rows in our dataset which include extra commas, and this is shifting values to the right.
# #Row 76 is an example of a bad row, let's return it by converting to a pandas dataframe and using the `iloc` integer location to return and print the row.
#%%
df = peopleData.to_pandas_dataframe()
problemRow1 = df.iloc[76, 0:10]
print(problemRow1)

#%% [markdown]
# In the csv file, the "ADDRESS" column value has unquoted commas and so its value has spilt onto adjacent columns.
# Also, we stumbled across row 14608 in the original csv, which has a speech mark at the start of the row which has cancelled out all of its following commas, so that the whole row contents lies within the "ADDRESS value".
# All of the data for this row is bunched up into the first column because of a lone speech mark in the original csv.
# Here is the value within the ADDRESS column:

#%%
rawFile = dprep.read_lines(folderPath)
df2 = rawFile.to_pandas_dataframe()
df2.get_value(14606,'Line')
# problemRow2 = df2.iloc[14606]
# print(problemRow2)

#%% [markdown]
### 1.3 Cleanse PEOPLE
# Let's continue anyway by removing the first row:
#%%
peopleData = peopleData.skip(1)
peopleData.head(5)

#%% [markdown]
# Now, let's attempt to separate the rows which have cells shifted over into new columns because of exta commas on those rows. These rows should be investigated separately. If we check which rows in the final 5 columns have non-empty values, then we should be capturing _most_ of the dodgy rows. The exceptional case would be for rows which include bad data, but whose last 5 column values are empty strings. These would not be picked up by the following:
#%%
quarantinedPeopleData1 = peopleData.filter(
    dprep.f_or(
        dprep.col('Column39') != '',
        dprep.col('Column40') != '',
        dprep.col('Column41') != '',
        dprep.col('Column42') != '',
        dprep.col('Column43') != ''))
quarantinedPeopleData1.head(10)

#%% [markdown]
# Let's grab the rows which seemingly have the correct number of columns:
#%%
goodPeopleData = peopleData.filter(
    dprep.f_and(
        dprep.col('Column39') == '',
        dprep.col('Column40') == '',
        dprep.col('Column41') == '',
        dprep.col('Column42') == '',
        dprep.col('Column43') == '')).drop_columns(dprep.ColumnSelector('Column'))

goodPeopleData.head(10)

#%% [markdown]
# Let's compare the counts for each dataflow.
#%%
originalPeopleDataCount = peopleData.row_count
goodPeopleDataCount = goodPeopleData.row_count
quarantinedPeopleDataCount = quarantinedPeopleData1.row_count

print('Row count checks\nOriginal people data: {0}\nGood people data: {1}\nDodgy people data: {2}'.format(originalPeopleDataCount, goodPeopleDataCount, quarantinedPeopleDataCount))
if goodPeopleDataCount + quarantinedPeopleDataCount == originalPeopleDataCount:
    print('Looks good, row counts are consistent')
else:
    print('Hmmm somethings not right, row counts do not tally!')

#%%[markdown]
#**ACTION** : need to come back to the approach to separating good out from quarnatined PEOPLE data to understand why row counts don't tally!
#
# Let's continue cleaning the good dataset. We have the custom string '<null'> representing our null value. Let's change that to `null`, along with any empty string in any of the columns:
#%%
goodPeopleDataColumns = list(goodPeopleData.get_profile().columns.keys())
goodPeopleData = goodPeopleData.replace(goodPeopleDataColumns, '<null>', None)
goodPeopleData = goodPeopleData.replace(goodPeopleDataColumns, '', None)
goodPeopleData.head(5)

#%% [markdown]
# Now replace the CR/LF `\0d0a` string with a comma in the address, and replace empty values with an empty string:
#%%
goodPeopleData = goodPeopleData.str_replace('ADDRESS', r'\0d0a', ', ')
goodPeopleData = goodPeopleData.replace('ADDRESS', r', , , ', None)
goodPeopleData.head(5)

#%% [markdown]
# These rows are the reason why the initial smart file read didn't end up being so smart. Would there be a way around this? Could the number of commas in a row be counted upon ingestion, and sectioned off to an "erroneous rows" table if there are too many commas? Could there be a way to add a schema definition to the auto_read_file function to give it a hint as to how many columns it's looking for? Is there some other function we can use here of which we're not aware?
# Let's attempt to convert our date columns from strings to datetimes:
#%%
goodPeopleData = goodPeopleData.to_datetime(columns = ['DATEDIED','DOB','GMPDATE', 'MARRDATE', 'MINRETIREMENTDATE', 'SPAD'], date_time_formats = ['%d/%m/%Y'])
goodPeopleData = goodPeopleData.to_number(['ID'])
goodPeopleData.head(5)

#%% [markdown]
# Now do the same for the key / private key columns:

#%%
#goodPeopleData = goodPeopleData.to_long('ID')
#goodPeopleData = goodPeopleData.to_long('MEMNO')
#to_integer(columns = ['PEOPLEID','MEMBERID'])
goodPeopleData.head(5)

#%% [markdown]
# Let's also check the profile of the data set we are now working with to see how it looks:

#%%
goodPeopleData.get_profile()

#%% [markdown]
# So nice to see things like:
# - No errors in any of the date columns hacing applied this type to the column
# - No missing values for DOB : date of birth
# - Lots of missing values for DATEDIED which is intuitive
# - Only 203 people with married date, doesn't seem right : MARRDATE

#%% [markdown]
# #### Investigate SEX column
# Find distinct values:

#%%
goodPeopleData.get_profile().columns['SEX'].value_counts

#%% [markdown]
# Looks good all either an M or an F, no anomolies or missing values.

#%% [markdown]
# #### Investigate TITLE column
# Find distinct values:

#%%
goodPeopleData.get_profile().columns['TITLE'].value_counts

#%% [markdown]
# Here we can see that there are a number of different ways of representing the likes of "Mrs", "Mrs.", "mrs"
# Lets see what fuzzy grouping can do:

#%%
builder = goodPeopleData.builders.fuzzy_group_column(source_column='TITLE',
                                       new_column_name='TITLE_grouped',
                                       similarity_threshold=0.8,
                                       similarity_score_column_name='TITLE_grouped_score')

builder.learn()
builder.groups

#%% [markdown]
# First pass with a threashold of 0.8 doesn't do so well, so let's try with a threshold of 0.9:
#%%
builder.similarity_threshold = 0.9
builder.learn()
builder.groups

#%% [markdown]
# So this cleans up the instances of "Mrs", "Mrs." and "mrs" + "Rev" and "REV"

#%%
goodPeopleData2 = builder.to_dataflow()
goodPeopleData2.get_profile().columns['TITLE_grouped'].value_counts

#%% [markdown]
# #### Investigate MSTA column
# Find distinct values:

#%%
goodPeopleData2.get_profile().columns['MSTA'].value_counts

#%% [markdown]
# Here we can see that there are a number of different ways of representing the likes of "Mrs", "Mrs.", "mrs"
# Let's see what fuzzy grouping can do:

#%%
builder = goodPeopleData2.builders.fuzzy_group_column(source_column='MSTA',
                                       new_column_name='MSTA_grouped',
                                       similarity_threshold=0.1,
                                       similarity_score_column_name='MSTA_grouped_score')

builder.learn()
builder.groups

#%% [markdown]
# So this cleans up the instances of "Serarated" and "Seperated", but despite placing the threashold as low as 0.1 it has not picked up the other example such as those around "Widowed" or "Civil Partner"

#%%
goodPeopleData3 = builder.to_dataflow()
goodPeopleData3.get_profile().columns['MSTA_grouped'].value_counts

#%% [markdown]


#%%
# goodPeopleData3 = goodPeopleData.filter(dprep.f_or(dprep.col('SEX') == 'M', dprep.col('SEX') == 'F'))
# goodPeopleData3.head(10)

#%% [markdown]
## MEMBER
# Let's take a look at the **MEMBERS.csv** file. We'll import it using the `auto_read_file` (new name for `smart_read_file` if we're not mistaken) function. This will automatically detect the file type and how to parse it. If we're lucky, it will detect the types of each column and apply any corresponding type transformations.
### 1.1 Ingest MEMBER
#%%
folderPath = './data/members.csv'
memberData = dprep.auto_read_file(folderPath)
memberData.head(5)

#%%
memberData.get_profile()

#%% [mardown]
# Unfortunately the **auto_read_file** seems to lose the column headings.  Doesn't seem to be ability to override this function, so will try **read_CSV** instead...

#%%
memberData = dprep.read_csv(folderPath)
memberData.head(5)

#%%
memberData.get_profile()

#%% [markdown]
# In raw form, it keeps promtes the header, but it:
# - Keeps the top row, and;
# - Doesn't detect the data types.
# Trying setting some of the arguments for **read_csv**

#%%
memberData = dprep.read_csv(folderPath, skip_rows=1, inference_arguments=dprep.InferenceArguments(day_first=False))
memberData.head(5)

#%% [markdown]
# Unfortunately **skip_rows** leads to the header row being dropped and then the first row being promoted to a header row.
# There is a **skip_mode** attirbute for **read_csv**, but struggling to find documentation for this.
# Will now encourage **read_csv** to detect data types:

#%%

memberData = dprep.read_csv(folderPath, inference_arguments=dprep.InferenceArguments(day_first=False))
memberData.head(5)

#%% [markdown]
# Now take out the first row:
#%%
memberData = memberData.skip(1)
memberData.head(5)


#%% [markdown]
# Clean up <null> entries...

#%%
memberDataColumns = list(memberData.get_profile().columns.keys())
memberData = memberData.replace(memberDataColumns, '<null>', None)
memberData = memberData.replace(memberDataColumns, '', None)

#%% [markdown]
# Let's get the columns into the right data types...

#%%
# memberData = memberData.to_long('MEMNO')
memberData = memberData.to_datetime(columns = ['CCLEFFDATE','CEPDATE','DCE', 'DCSTA', 'DJC', 'DJS', 'DNOMS', 'DOELL', 'DOELP', 'DOL', 'NRD', 'PSERVST', 'STATED', 'TARGDAT'], date_time_formats = ['%d/%m/%Y'])
memberData = memberData.to_number(['PEOPLEID'])
memberData.head(5)

#%%
memberData.get_profile()

#%% [markdown]
## Stage 2 - Join Data

#%%
join_builder = goodPeopleData3.builders.join(right_dataflow=memberData, left_column_prefix='l', right_column_prefix='r')
join_builder.detect_column_info()
join_builder

#%%
join_builder.generate_suggested_join()
join_builder.list_join_suggestions()

#%% [markdown]
# Weird, it doesn't come up with a suggestion despite having two MEMNO integer columns to work with!

#%%
people_memberJoined = dprep.Dataflow.join(left_dataflow=goodPeopleData3,
                                      right_dataflow=memberData,
                                      join_key_pairs=[('ID', 'PEOPLEID')],
                                      left_column_prefix='l_',
                                      right_column_prefix='r_')

#%%
people_memberJoined.head(5)
#%% [markdown]
## Stage 3 - Map To Canonical Form
# Map the joined up data onto a canonical form.
# Haven't figured out how best to do this yet!
# It would be great if you could define some canonical form, or create from an existing file / database table schema?
# Then you could apply some ML to infer / learn how to map?

#%% [markdown]
## Stage 4 - Run Generic Data Quality
# Now we're at the stage where we can start to apply generic data quality checks...
### 4.1 - Date Checks

#### 4.1.1 - Check : Date Joined Company is after Date Joined Scheme

#%%
people_memberJoined = people_memberJoined.add_column(new_column_name='DQC_DJS_greaterThan_DJC',
                           prior_column='r_DJS',
                           expression=people_memberJoined['DJS'] > people_memberJoined['DJS'])

#%%
people_memberJoined = people_memberJoined.add_column(new_column_name='DQC_Test2',
                           prior_column='r_DJS', expression='')


#%%
people_memberJoined.add_col

#%%
people_memberJoined = people_memberJoined.new_script_column(new_column_name='test2', insert_after='DQC_DJS_greaterThan_DJC', script="""
def newvalue(row):
    return 'ERROR - DJS earlier than DJC'
""")

#%%
people_memberJoined.get_profile()

#%%
people_memberJoined.head(20)

#%% [markdown]
#### 4.1.2 - Date of Birth is after Date Joined Scheme
