import pandas as pd
import azureml.dataprep as dprep
from commonPackageHandling import openDataFlowPackage, saveDataFlowPackage
from commonInventoryCreation import getColumnStats, getDataFlowStats

def joinTables(dataName, previousStageNumber, thisStageNumber, qualityFlag, operatorToUse, operationFlag):
    
    dataFlow, fullPackagePath = openDataFlowPackage(dataName, previousStageNumber, qualityFlag)
    
    if dataFlow:

        print('{0}: loaded package from path {1}'.format(dataName, fullPackagePath))

        # Set up empty intermediate dataframes that we will use to build up inventories at both dataFlow and column level
        dataFlowInventoryIntermediate = pd.DataFrame()
        columnInventoryIntermediate = pd.DataFrame()

        if operationFlag != '':
            
            # Load config file
            joinConfig = dprep.read_csv('./Config/' + operationFlag).to_pandas_dataframe()
            
            # For each config in the file...
            for index, row in joinConfig.iterrows():
                
                leftDataName = row['LeftDataName']
                leftDataFlowJoinColumn = row['LeftDataFlowJoinColumn']
                rightDataName = row['RightDataName']
                rightDataFlowJoinColumn = row['RightDataFlowJoinColumn']
                print('{0}: ready to join {1} {2} -> {3} {4}'.format(dataName, leftDataName, leftDataFlowJoinColumn, rightDataName, rightDataFlowJoinColumn))

                # Load right hand data flow
                rightDataFlow, fullPackagePath = openDataFlowPackage(rightDataName, previousStageNumber, qualityFlag)
                print('{0}: loaded package from path {1}'.format(rightDataName, fullPackagePath))

                # Perform the join
                join_builder = dataFlow.builders.join(right_dataflow=rightDataFlow, 
                                            left_column_prefix=dataName + '_',
                                            right_column_prefix=rightDataName + '_')
                join_builder.detect_column_info()
                join_builder.join_key_pairs=[(leftDataFlowJoinColumn, rightDataFlowJoinColumn)]
                # Setting up join type:
                # NONE = 0
                # MATCH = 2
                # UNMATCHLEFT = 4
                # UNMATCHRIGHT = 8
                join_builder.join_type = 2
                newDataFlow = join_builder.to_dataflow()

                # Create a new name for this data flow based on concatenation of left dataflow and right
                newDataName = dataName + '_' + rightDataName

                # Output key stats
                print('{0} left table : {0}, Columns : {1}, Rows : {2}'.format(leftDataName, len(dataFlow.get_profile().columns), dataFlow.row_count))
                print('{0} right table : {0}, Columns : {1}, Rows : {2}'.format(rightDataName, len(rightDataFlow.get_profile().columns), rightDataFlow.row_count))

                newDataProfile = newDataFlow.get_profile()

                print('{0} joined table : {0}, Columns : {1}, Rows : {2}'.format(newDataName, len(newDataProfile.columns), newDataFlow.row_count))

                # Now generate column and data flow inventories
                columnInventory = getColumnStats(newDataProfile, newDataName, thisStageNumber, operatorToUse, operationFlag)
                dataFlowInventory = getDataFlowStats(newDataFlow, newDataProfile, newDataName, thisStageNumber, operatorToUse, operationFlag)

                # Capture the column inventory for the new dataflow
                columnInventoryIntermediate = columnInventoryIntermediate.append(columnInventory)

                # Capture the data flow inventory for the new data flow
                dataFlowInventoryIntermediate = dataFlowInventoryIntermediate.append(dataFlowInventory)
                
                # Finally save the data flow so it can be passed onto the next stage of the process...
                targetPackagePath = saveDataFlowPackage(newDataFlow, newDataName, thisStageNumber, 'A')
                print('{0}: saved package to {1}'.format(newDataName, targetPackagePath))


        else:
            print('{0}: no joining of tables required'.format(dataName))

        dataProfile = dataFlow.get_profile()

        # Now generate column and data flow inventories
        columnInventory = getColumnStats(dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        columnInventoryIntermediate = columnInventoryIntermediate.append(columnInventory)

        dataFlowInventory = getDataFlowStats(dataFlow, dataProfile, dataName, thisStageNumber, operatorToUse, operationFlag)
        dataFlowInventoryIntermediate = dataFlowInventoryIntermediate.append(dataFlowInventory)

        # Finally save the data flow so it can be passed onto the next stage of the process...
        targetPackagePath = saveDataFlowPackage(dataFlow, dataName, thisStageNumber, qualityFlag)
        print('{0}: saved package to {1}'.format(dataName, targetPackagePath))

        return dataFlow, columnInventoryIntermediate, dataFlowInventory

    else:
        print('{0}: no package file found at location {1}'.format(dataName, fullPackagePath))
        return None, None, None
