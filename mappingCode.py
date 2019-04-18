# Aim of this experiment is to see if we can do configuration based transformation of data
# For example if we had a configuration file that had the following lines in it:
# SEX, ->, UPM.Sex
# TITLE, ->, UPM.Title
# In the example above -> means "maps to" and this will look for files called SEX_MAP.csv and TITLE_MAP.csv
# it will then use the values in these files to map values from the SOURCE to the DESTINATION

import pandas as pd
import os
import csv

packagePath = "./packages"

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

def createConfigFromDataFlow(dataFlow, dataName):
    configSourceColumnNames = []
    configMappingTypes = []
    configTargetColumnNames = []

    for configCol in list(dataFlow.get_profile().columns.keys()):
        configSourceColumnNames.append(configCol)
        configMappingTypes.append('=>')
        configTargetColumnNames.append('UPM.' + configCol)

    config = pd.DataFrame()
    configSourceColumns = pd.DataFrame({'SourceColumn':configSourceColumnNames})
    config = pd.concat([config, configSourceColumns], axis=1)
    configMapping = pd.DataFrame({'MappingType':configMappingTypes})
    config = pd.concat([config, configMapping], axis=1)
    configTargetColumn = pd.DataFrame({'TargetColumn':configTargetColumnNames})
    config = pd.concat([config, configTargetColumn], axis=1)

    configPath = packagePath + '/' + dataName + '/' + dataName + '_Config.csv'
    config.to_csv(configPath, index = None)
    return configPath

def createDummyConfigFromDataFlow(dataFlow, dataName):
    config = pd.DataFrame()
    if dataName == 'PEOPLE':
        configSourceColumnNames = []
        configMappingTypes = []
        configTargetColumnNames = []

        for configCol in ['SEX', 'TITLE']:
            configSourceColumnNames.append(configCol)
            configMappingTypes.append('->')
            configTargetColumnNames.append('UPM.' + configCol)

        config = pd.DataFrame()
        configSourceColumns = pd.DataFrame({'SourceColumn':configSourceColumnNames})
        config = pd.concat([config, configSourceColumns], axis=1)
        configMapping = pd.DataFrame({'MappingType':configMappingTypes})
        config = pd.concat([config, configMapping], axis=1)
        configTargetColumn = pd.DataFrame({'TargetColumn':configTargetColumnNames})
        config = pd.concat([config, configTargetColumn], axis=1)

        configPath = packagePath + '/' + dataName + '/' + dataName + '_Config.csv'
        config.to_csv(configPath, index = None)

    configPath = packagePath + '/' + dataName + '/' + dataName + '_Config.csv'
    config.to_csv(configPath, index = None)
    return configPath

def createUPMMappingConfigFromDataFlow(dataFlow, dataName):
    configSourceTableNames = []
    configSourceColumnNames = []
    configTargetTableNames = []
    configTargetColumnNames = []

    configPath = packagePath + '/' + dataName + '/' + dataName + '_Target_Mapping_Config.csv'

    if os.path.isfile(configPath):
        return configPath

    for configCol in list(dataFlow.get_profile().columns.keys()):
        configSourceTableNames.append(dataName)
        configSourceColumnNames.append(configCol)
        configTargetTableNames.append('UPM.' + dataName)
        configTargetColumnNames.append('UPM_' + configCol)

    config = pd.DataFrame()
    configSourceTables = pd.DataFrame({'SourceTable':configSourceTableNames})
    config = pd.concat([config, configSourceTables], axis=1)
    configSourceColumns = pd.DataFrame({'SourceColumn':configSourceColumnNames})
    config = pd.concat([config, configSourceColumns], axis=1)
    configTargetTables = pd.DataFrame({'TargetTable':configTargetTableNames})
    config = pd.concat([config, configTargetTables], axis=1)
    configTargetColumn = pd.DataFrame({'TargetColumn':configTargetColumnNames})
    config = pd.concat([config, configTargetColumn], axis=1)
    
    config.to_csv(configPath, index = None)
    return configPath