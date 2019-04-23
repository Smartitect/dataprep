#%% [markdown]
# We can immediately see that we have some problems with our data. Our first row isn't helpful. We have the new line code `\0d0a` in numerous columns.
# We also have extra columns defined in our dataset that we aren't expecting. This is possibly because we have rows in our dataset which include extra commas, and this is shifting values to the right.
# #Row 76 is an example of a bad row, let's return it by converting to a pandas dataframe and using the `iloc` integer location to return and print the row.
#%%
df = peopleDataFlow.to_pandas_dataframe()
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

