"""Main module."""

import logging
import pandas as pd
import datetime
from datetime import timedelta
import json
import sys

    # getting config connection data
try:
        logging.info("Starting to fetch connection details from config file")
        connection_config_filepath = '/mnt/c/Users/189319/Downloads/.ipynb_checkpoints/cookiecutter/cookie_doc/cookie_doc/config_fuel_service.json'
        with open(connection_config_filepath) as config_file:
            connection_config = json.load(config_file)

        
        fuelAmount_threshold = connection_config["fuelAmount_threshold"]
        eventDateTime_threshold = connection_config["eventDateTime_threshold"]
        

        logging.info("fetching connection details from config file successful")
except Exception:
        logging.error("Error in source configuration file", exc_info=True)
        logging.critical("Code failed to execute")
        sys.exit()




def merge_excel_files(file1_path, file2_path, merge_on, column_list, format_type='list'):
    """
    Merges two Excel files on specified columns and performs column comparisons to create new columns.

    Parameters:
        file1_path (str): The path to the first Excel file to merge.
        file2_path (str): The path to the second Excel file to merge.
        merge_on (list of str): A list of column names to merge the two dataframes on.
        column_list (list of str): A list of column names to perform comparisons on.
        format_type (str): A string indicating the format in which the new columns should be returned. The default is 'list'.

    Returns:
        pandas.DataFrame: The merged dataframe with the new columns.
    """
    # Read in the two Excel files
    try:
        df1 = pd.read_excel(file1_path)
        df2 = pd.read_excel(file2_path)
    except FileNotFoundError:
        print("One or both of the files could not be found.")
        return None
    except Exception as e:
        print("An error occurred while reading in the files: " + str(e))

        return None

    # Check that the specified columns exist in both dataframes
    for col in merge_on:
        if col not in df1.columns or col not in df2.columns:
            print("The column '%s' does not exist in both dataframes." % col)

            return None
    for col in column_list:
        if col+'_x' not in df1.columns or col+'_y' not in df2.columns:
            print("The columns '%s_x' and/or '%s_y' do not exist in both dataframes." % (col, col))

            return None

    # Merge the two dataframes on the columns that have the same values
    merged_df = pd.merge(df1, df2, on=merge_on)

    # Loop through the column mappings and create new columns based on the comparison
    for column_name in column_list:
        merged_df[column_name] = merged_df.apply(lambda row: [row[column_name+'_x'], row[column_name+'_y']], axis=1)

    # Drop the original columns from the merged dataframe
    merged_df.drop([column_name + "_x" for column_name in column_list] + [column_name + "_y" for column_name in column_list], axis=1, inplace=True)


    # Return the merged dataframe with the new columns
    return merged_df


column_list=[ 
        "eventType",  
        "startDateTime",
        "eventDateTime", 
        "fuelAmount",
        "fuelConsumption",  
        "engineHours", 
        "fuelLevelBeforeEvent", 
        "fuelLevelAfterEvent",  
        "tankCapacity"
    ]

merged_df = merge_excel_files('/mnt/c/Users/189319/Downloads/.ipynb_checkpoints/cookiecutter/Input_FuelService1.xlsx', '/mnt/c/Users/189319/Downloads/.ipynb_checkpoints/cookiecutter/Input_FuelService2.xlsx', ['vin', 'vehRegNo', 'odometer'], column_list)




def detect_fuel_anomalies(df, fuel_threshold, time_threshold):
    # Convert fuelAmount column to list of integers
    df['fuelAmount'] = df['fuelAmount'].apply(lambda x: [int(i) for i in x])
    
    # Compute fuelAmount difference and filter out rows with difference below threshold
    df['fuelAmount_diff'] = df['fuelAmount'].apply(lambda x: {'fuelAmount': x} if abs(x[1] - x[0])/x[1] > fuel_threshold else {})
    df['fuelAmount_diff'] = df['fuelAmount_diff'].apply(lambda x: x if isinstance(x, dict) and x != {} else None)
    
    # Compute time difference and filter out rows with difference below threshold

    df['timeDifference'] = df['eventDateTime'].apply(lambda x: {'eventDateTime': x} if abs(x[1] - x[0]) > timedelta(minutes=time_threshold) else {})
    df['timeDifference'] = df['timeDifference'].apply(lambda x: x if x != {} else None)
    
    return df

merged_df=detect_fuel_anomalies(merged_df, fuelAmount_threshold,eventDateTime_threshold)

def combine_dicts(x):
    dicts = [col for col in [ x['timeDifference'], x['fuelAmount_diff']] if col is not None]
    result= {k: v for d in dicts for k, v in d.items() }
    if result !={}:
        output=result
    else:
        output=None
    return output
        
def final_data(merged_df):
    merged_df['diff_details_by_threshold'] = merged_df.apply(combine_dicts, axis=1)
    merged_df= merged_df.drop(['fuelAmount_diff', 'timeDifference'], axis=1)
    merged_df.to_excel('/mnt/c/Users/189319/Downloads/.ipynb_checkpoints/cookiecutter/output_result.xlsx', index=False)

final_data(merged_df)



