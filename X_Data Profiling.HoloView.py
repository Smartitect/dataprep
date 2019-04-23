#%% [markdown]
# # Data Profiling Playbook
# The intention of this notebook is to develop a generic approach to profiling a single column.
# This will take into account:
# - The primary data type of the column - eg if it's a date, we can generate more specialised analytics
# - We may also be able to apply more specialised checks, for example based on REGEX to check national insurance numbers etc.

#%%
# Import all of the libraries we need to use...
import pandas as pd
import azureml.dataprep as dprep
import os as os
import re as re
import collections
import pandas_profiling as pp
import datetime
import holoviews as hv
import numpy as np
from holoviews import opts
from azureml.dataprep import value
from azureml.dataprep import col
from azureml.dataprep import Dataflow
from commonPackageHandling import openDataFlowPackage

#%%
hv.extension('bokeh')
facets = ['day', 'month', 'year']

def return_year(x):
    
    if not isinstance(x, datetime.datetime):
        return 0
    if x.year is None:
        return 0

    return x.year

def return_month(x):

    if not isinstance(x, datetime.datetime):
        return 0
    if x.month is None:
        return 0

    return x.month

def return_day(x):

    if not isinstance(x, datetime.datetime):
        return 0
    if x.day is None:
        return 0

    return x.day

def return_date(x):
    
    if not isinstance(x, datetime.datetime):
        return ''
    if x.day is None:
        return ''

    return x

#%%
dataFlow, dataFlowPath = openDataFlowPackage('PEOPLE', '50', 'A')
df = dataFlow.to_pandas_dataframe().head(1000)

profile = dataFlow.get_profile()

dob = df['DOB']

to_days = np.vectorize(lambda x: return_day(x))
to_months = np.vectorize(lambda x: return_month(x))
to_years = np.vectorize(lambda x: return_year(x))
to_date = np.vectorize(lambda x: return_date(x))

#%%


histograms = []
#%%
for f in facets:
    if f == 'day':
        dayHisto = np.histogram(to_days(dob), bins=31, range=(1, 31))
        histograms.append(hv.Histogram(dayHisto, kdims=['Day of Birth'], height=500, width=500))
         
    if f == 'month':
        monthHisto = np.histogram(to_months(dob), bins=12, range=(1, 12))
        histograms.append(hv.Histogram(monthHisto, kdims=['Month of Birth'], height=500, width=500))

    if f == 'year':
        yearHisto = np.histogram(to_years(dob), bins=200)
        histograms.append(hv.Histogram(yearHisto, kdims=['Year of Birth'], height=500, width=500))

    if f == 'date':
        dateHisto = np.histogram(to_date(dob), bins=200)
        histograms.append(hv.Histogram(dateHisto, kdims=['Dates of Birth'], height=500, width=500))

layout = hv.Layout()
#%%
for h in histograms:
    layout = layout + h

df_html = df[['DOB']].describe().to_html()
df_div = hv.Div("<div align='right'>"+df_html+"<div>")

layout = layout + df_div

#%%
hv.save(layout, 'dobHistogram.html')

