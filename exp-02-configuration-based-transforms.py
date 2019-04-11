# Aim of this experiment is to see if we can do configuration based transformation of data
# For example if we had a configuration file that had the following lines in it:
# SEX, ->, UPM.Sex
# TITLE, ->, UPM.Title
# In the example above -> means "maps to" and this will look for files called SEX_MAP.csv and TITLE_MAP.csv
# it will then use the values in these files to map values from the SOURCE to the DESTINATION

import pandas as pd
import os
import csv

def load_etl_configuration(configPath):
    config = []
    with open(configPath) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            config.append(row)

    # pd.DataFrame(data=config[1:, 1:], columns=config[0, 1:])
    config_df = pd.DataFrame.from_records(config)
    new_df = pd.DataFrame(config_df.values[1:], columns=config_df.iloc[0])

    return new_df

def csv_to_dictionary(csvPath):
    with open(csvPath, mode='r') as infile:
        reader = csv.reader(infile)
        map = {rows[0]: rows[1] for rows in reader}
    return map

def load_transformation_configuration(configPath):
    transforms = []
    with open(configPath) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            transforms.append(row)
    return transforms

def filter_maps_from_config(config):
    mapFilter = filter(lambda x: x[1] == '->',  config)
    maps = []
    for row in mapFilter:
        maps.append(row[0])
    return maps

def get_lookups_from_transforms(transforms):
    filtered = filter_maps_from_config(transforms)
    maps = {rows: csv_to_dictionary(os.path.join(
        os.getcwd(), 'Maps', rows + '_MAP.csv')) for rows in filtered}
    return maps

def apply_lookup(key, lookup_dictionary):
    # we return either the value assigned to the key,
    # or the default value as defined in the mapping file
    return lookup_dictionary.get(key, lookup_dictionary.get("*"))

def get_destination_column_name(source_column_name, config):
    for setting in config:
        if setting[0] == source_column_name:
            return setting[2]

ETL_CONFIG_PATH = os.path.join(os.getcwd(), 'CONFIG', 'ETL.csv')

etl_config = load_etl_configuration(ETL_CONFIG_PATH)

for index, row in etl_config.iterrows():
    #print(row['DATA'], row['CONFIG'])

    DATA_PATH = os.path.join(os.getcwd(), row['DATA'])
    CONFIG_PATH = os.path.join(os.getcwd(), row['CONFIG'])

    transforms = load_transformation_configuration(CONFIG_PATH)
    lookups = get_lookups_from_transforms(transforms)

    # We also want to skip the first row, as this contains junk
    df = pd.read_csv(DATA_PATH, skiprows=[1])
    df2 = pd.DataFrame()

    for key in lookups:
        destination_column = get_destination_column_name(key, transforms)
        df2[destination_column] = df[key].apply(apply_lookup, lookup_dictionary=lookups[key])
        # next, apply copy / filter / ignore transforms

    # If you want to see the entire dataframe
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #    print(df2)

    output_file_name = (row['DATA'].split("\\")[-1].split(".")[0])

    print(output_file_name)

    df2.to_pickle(os.path.join(os.getcwd(), 'output',  output_file_name + '.pickle'))