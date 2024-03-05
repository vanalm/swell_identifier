#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 11:28:17 2024

@author: jacobvanalmelo
test swell detection with real-time data
"""

import requests
from lxml import html
import pandas as pd
from datetime import timedelta, datetime
from dateutil import parser
import time
import datetime
import uuid



window_size = 3
variability_factor = 3  # Adjust based on further analysis of the data
min_jump = 2  # Minimum jump to consider as a swell start
sustain_window = 2  # Number of values to confirm the swell condition is sustained

BUOY_IDS = ['51101', '51208', '51201', '51210', '51205', '51206']
BUOY_MAP = {
    '51101': 'H2 (186 NM NW of Kauai)',
    '51208': 'Hanalei',
    '51201': 'Waimea Bay',
    '51210': 'Kaneohe Bay',
    '51205': 'Pauwela',
    '51206': 'Hilo'
}
COLS_TO_KEEP = ['time', 'WVHT', 'DPD', 'MWD']


def scrape_buoy_data(buoy_id):
    url = f'https://www.ndbc.noaa.gov/station_page.php?station={buoy_id}'
    response = requests.get(url)
    tree = html.fromstring(response.content)
    rows = tree.xpath('//*[@id="wxdata"]/div/table/tbody/tr')
    
    data = []
    for row in rows:
        time = ' '.join(row.xpath('.//th//text()')).strip()
        values = [value.strip() for value in row.xpath('.//td/text()')]
        data.append([time] + values)

    headers = ['time', 'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD', 'PRES', 'PTDY', 'ATMP', 'WTMP', 'DEWP', 'SAL', 'VIS', 'TIDE']
    df = pd.DataFrame(data, columns=headers)
    return df.loc[:, COLS_TO_KEEP]

def scrape_buoy_data(buoy_id):
    url = f'https://www.ndbc.noaa.gov/station_page.php?station={buoy_id}'
    response = requests.get(url)
    tree = html.fromstring(response.content)
    rows = tree.xpath('//*[@id="wxdata"]/div/table/tbody/tr')
    
    data = []
    for row in rows:
        time = ' '.join(row.xpath('.//th//text()')).strip()
        values = [value.strip() for value in row.xpath('.//td/text()')]
        data.append([time] + values)

    headers = ['time', 'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD', 'PRES', 'PTDY', 'ATMP', 'WTMP', 'DEWP', 'SAL', 'VIS', 'TIDE']
    df = pd.DataFrame(data, columns=headers)
    return df.loc[:, COLS_TO_KEEP]

def is_swell_now(df, column_name, window, variability_factor, min_jump, sustain_window=0):
    '''
    Modifies the df by adding a 'swell_start' column, marking 1 if a swell start is detected and
    optionally confirmed within the sustain_window, otherwise 0. Also, evaluates if the latest reading indicates a swell.
    
    Parameters:
    - df: DataFrame containing the data.
    - column_name: Name of the column to analyze for swell starts.
    - window: Number of previous values to consider for calculating mean and variability.
    - variability_factor: Multiplier for the standard deviation to set the threshold for detecting swells.
    - min_jump: Minimum increase over the mean required to consider a value as indicating a swell start.
    - sustain_window: Optional. Number of values following a detected swell start within which the swell conditions must be met again to confirm.
    
    Returns:
    - Tuple: (swell_detected_in_latest_reading, latest_readings, modified_df)
    '''
    # Initialize the 'swell_start' column with 0s
    df['swell_start'] = 0
    swell_detected_in_latest_reading = 0  # Default: No swell detected in the latest reading

    for i in range(window, len(df)):
        window_mean = df[column_name].iloc[i-window:i].mean()
        window_std = df[column_name].iloc[i-window:i].std()
        threshold = window_mean + (window_std * variability_factor)

        if df[column_name].iloc[i] > threshold and (df[column_name].iloc[i] - window_mean) >= min_jump:
            if sustain_window > 0:
                for j in range(1, sustain_window + 1):
                    if i+j < len(df) and df[column_name].iloc[i+j] - window_mean >= min_jump:
                        df.at[i, 'swell_start'] = 1
                        break
            else:
                df.at[i, 'swell_start'] = 1
                if i == len(df) - 1:  # Check if this is the latest reading
                    swell_detected_in_latest_reading = 1

    # Compile the latest readings
    latest_readings = {
        'time': df['time'].iloc[-1],  # Assuming 'time' is the column name for time stamps
        'WVHT': df['WVHT'].iloc[-1],
        'DPD': df['DPD'].iloc[-1],
        'MWD': df['MWD'].iloc[-1]
    }

    # Latest index is considered as len(df) - 1, meaning the last row in the DataFrame
    return swell_detected_in_latest_reading, latest_readings#, df
def prepare_and_analyze_data(buoy_id):
    df = scrape_buoy_data(buoy_id)
    # Convert necessary columns to numeric, leaving 'time' and 'MWD' as strings
    df['WVHT'] = pd.to_numeric(df['WVHT'], errors='coerce')
    df['DPD'] = pd.to_numeric(df['DPD'], errors='coerce')
    df.dropna(subset=['DPD'], inplace=True)  # Ensure analysis is possible
    
    if len(df) < window_size + sustain_window:
        return None  # Not enough data for analysis
    
    swell_detected, latest_readings = is_swell_now(df, 'DPD', window_size, variability_factor, min_jump, sustain_window)
    result = {
        'buoy_id': buoy_id, 
        'buoy_name': BUOY_MAP.get(buoy_id, "Unknown buoy"),
        'swell': swell_detected, 
        **latest_readings
    }
    return result



def check_for_swells(buoy_ids = BUOY_IDS):
    results = []
    for buoy_id in buoy_ids:
        result = prepare_and_analyze_data(buoy_id)
        if result:
            results.append(result)
    return results

def prepare_data(df):
    # Convert necessary columns to numeric, leaving 'time' and 'MWD' as strings
    df['WVHT'] = pd.to_numeric(df['WVHT'], errors='coerce')
    df['DPD'] = pd.to_numeric(df['DPD'], errors='coerce')
    df.dropna(subset=['DPD'], inplace=True)  # Ensure analysis is possible
    
    if len(df) < window_size + sustain_window:
        return None  # Not enough data for analysis
    return df

df = scrape_buoy_data('51205')
df = prepare_data(df)
df.at[0, 'DPD']=18
# df.at[1, 'DPD']=18
# df.at[2, 'DPD']=18

# flip dataframe
df = df.iloc[::-1].reset_index(drop=True)


swell_detected, latest_readings = is_swell_now(df, 'DPD', window_size, variability_factor, min_jump, sustain_window)
print(swell_detected)
print(latest_readings)
df.head()
# immitate a swell hitting now:

