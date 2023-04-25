#!/usr/bin/env python

"""Tests for `cookie` package."""


"""Tests for `cookie_templatedemo` package."""


import pytest
from cookie.cookie import merge_excel_files,detect_fuel_anomalies,combine_dicts
import pandas as pd
from datetime import datetime, timedelta


def test_merge_excel_files():
    # Input file paths for testing
    file1_path = 'C:/189235/mlop/ml_cookiecutterdemo/data/raw/Input_FuelService1.xlsx'
    file2_path = 'C:/189235/mlop/ml_cookiecutterdemo/data/raw/Input_FuelService2.xlsx'

    # Columns to merge on
    merge_on = ['vin', 'vehRegNo', 'odometer']

    # Column list for comparison
    column_list = [
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

    # Call the function to merge the files
    merged_df = merge_excel_files(file1_path, file2_path, merge_on, column_list)

    # Assertions to check the output
    assert isinstance(merged_df, pd.DataFrame)  # Check if output is a DataFrame
    assert set(column_list).issubset(merged_df.columns)  # Check if all columns are present in the merged dataframe
    assert merged_df.shape[0] > 0  # Check if merged dataframe has rows greater than 0

    # Check if original columns are dropped from the merged dataframe
    assert all(f"{column_name}_x" not in merged_df.columns for column_name in column_list)
    assert all(f"{column_name}_y" not in merged_df.columns for column_name in column_list)




def test_combine_dicts():
    # Test case 1: Both fuelAmount_diff and timeDifference are not None
    x1 = {'timeDifference': {'eventDateTime': [datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 2, 0)]},
          'fuelAmount_diff': {'fuelAmount': [100, 200]}}
    expected_output1 = {'eventDateTime': [datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 2, 0)],
                        'fuelAmount': [100, 200]}
    assert combine_dicts(x1) == expected_output1

    # Test case 2: Only fuelAmount_diff is not None
    x2 = {'timeDifference': None,
          'fuelAmount_diff': {'fuelAmount': [150, 250]}}
    expected_output2 = {'fuelAmount': [150, 250]}
    assert combine_dicts(x2) == expected_output2

    # Test case 3: Only timeDifference is not None
    x3 = {'timeDifference': {'eventDateTime': [datetime(2023, 1, 1, 3, 0), datetime(2023, 1, 1, 4, 0)]},
          'fuelAmount_diff': None}
    expected_output3 = {'eventDateTime': [datetime(2023, 1, 1, 3, 0), datetime(2023, 1, 1, 4, 0)]}
    assert combine_dicts(x3) == expected_output3

    # Test case 4: Both fuelAmount_diff and timeDifference are None
    x4 = {'timeDifference': None, 'fuelAmount_diff': None}
    expected_output4 = None
    assert combine_dicts(x4) == expected_output4




def test_detect_fuel_anomalies():
    # Create sample dataframe for testing
    df = pd.DataFrame({'fuelAmount': [[100, 200], [150, 150], [200, 300]],
                       'eventDateTime': [[datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 2, 0)],
                                         [datetime(2023, 1, 1, 3, 0), datetime(2023, 1, 1, 4, 0)],
                                         [datetime(2023, 1, 1, 5, 0), datetime(2023, 1, 1, 6, 0)]]})

    # Test case 1: Test with fuel and time thresholds that trigger anomalies
    fuel_threshold = 0.02
    time_threshold = 30
    expected_output1 = pd.DataFrame({'fuelAmount': [[100, 200], [150, 150], [200, 300]],
                                    'eventDateTime': [[datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 2, 0)],
                                                    [datetime(2023, 1, 1, 3, 0), datetime(2023, 1, 1, 4, 0)],
                                                    [datetime(2023, 1, 1, 5, 0), datetime(2023, 1, 1, 6, 0)]],
                                    'fuelAmount_diff': [{'fuelAmount': [100, 200]}, None, {'fuelAmount': [200, 300]}],
                                    'timeDifference':  [{'eventDateTime': [datetime(2023, 1, 1, 1, 0), datetime(2023, 1, 1, 2, 0)]},
                                {'eventDateTime': [datetime(2023, 1, 1, 3, 0), datetime(2023, 1, 1, 4, 0)]},
                                {'eventDateTime': [datetime(2023, 1, 1, 5, 0), datetime(2023, 1, 1, 6, 0)]}]})

    assert detect_fuel_anomalies(df, fuel_threshold, time_threshold).equals(expected_output1)

