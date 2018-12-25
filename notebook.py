#%% [markdown]
##Stage 1 - Ingest
# Let's start by importing the data prep SDK and pandas library:
#%%
import pandas as pd
import azureml.dataprep as dprep
import seaborn as sns
import os as os
from azureml.dataprep import value

#%% [markdown]
#---
## Stage 1 : Read Data
### 1.1 Ingest PEOPLE
# Let's take a look at the **PEOPLE.csv** file. We'll import it using the `auto_read_file` (new name for `smart_read_file` if we're not mistaken) function. This will automatically detect the file type and how to parse it. If we're lucky, it will detect the types of each column and apply any corresponding type transformations.

#%%
path = "./data"
dirs = os.listdir( path )
fileList = []

for file in dirs:
    fullFilePath = path + "/" + file
    if fullFilePath.endswith(".csv") or fullFilePath.endswith(".csv"):
        fileList.append(fullFilePath)
        globals()[file] = dprep.read_csv(fullFilePath)




fileList



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
