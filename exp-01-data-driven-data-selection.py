# In this spike I wanted to prove that we could make the selection of
# columns in a CSV a data driven process.
# We select the columns we want to process from \config\members.csv
# which contains 3 fields.
# We read the CSV into an array and use this to filter the columns
# we want to load into the data frame.
# we then serialize out the new data frame into pickle format so it
# could be used by the next step.

import pandas as pd
import os
import csv

MEMBERS_CSV_PATH = os.path.join(os.getcwd(), 'Data', 'MEMBERS.csv')
MEMBERS_CONFIG_PATH = os.path.join(os.getcwd(), 'Config', 'MEMBERS.csv')

headers = []
with open(MEMBERS_CONFIG_PATH) as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        headers.append(row)

# We also want to skip the first row, as this contains junk
df = pd.read_csv(MEMBERS_CSV_PATH, skiprows=[1], usecols=headers[0])

#Display the data frame
df

df.to_pickle(os.path.join('.', 'Output', 'MEMBERS.pickle'))