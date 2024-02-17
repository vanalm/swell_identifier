#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 14:39:39 2024


@author: jacobvanalmelo

get data for swell identification, 
clean data
determine whether there is a swell or not

"""
import numpy as np
import requests
from lxml import html
import pandas as pd
from tqdm import tqdm
import pickle
from datetime import timedelta, datetime
from dateutil import parser

# buoy_ids = ['51101', '51000', '51201']
buoy_ids = ['51205']
cols_to_keep = ['time', 'WVHT', 'DPD', 'MWD']

window_size = 3
variability_factor = 3  # Adjust based on further analysis of the data
min_jump = 2  # Minimum jump to consider as a swell start
sustain_window = 2  # Number of values to confirm the swell condition is sustained

def scrape_buoy_data(buoy_id):
    '''
    This function gets all of the last 12 houra of data from a buoy from national buoy datacenter
    '''
    url = f'https://www.ndbc.noaa.gov/station_page.php?station={buoy_id}'
    response = requests.get(url)
    
    # Parse the HTML
    tree = html.fromstring(response.content)

    # Extract table rows
    rows = tree.xpath('//*[@id="wxdata"]/div/table/tbody/tr')
    data = []
    for row in rows:
        # Get the time from the <th> element
        time = row.xpath('.//th//text()')
        time = ' '.join(time).strip()
        
        # Get the data from <td> elements
        values = row.xpath('.//td/text()')
        cleaned_values = [value.strip() for value in values]
        
        # Combine time with the rest of the data
        row_data = [time] + cleaned_values
        data.append(row_data)

    # Create a DataFrame
    # Assuming the first row of the data contains headers
    headers = ['time','WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'APD', 'MWD', 'PRES', 'PTDY', 'ATMP', 'WTMP', 'DEWP', 'SAL', 'VIS', 'TIDE']
    df = pd.DataFrame(data, columns=headers)
    setattr(df, 'name', 'buoy'+buoy_id[-3:])

    return df

def get_buoys(buoys, show=False):
    '''
    outputs a list of dfs to be used to predict
    '''
    dfxs = []
        
    for buoy_id in buoys:
        dfxs.append(scrape_buoy_data(buoy_id))
        
        # double check that I got what I thought I did.
    if show:
        for df in dfxs:
            print(df.head(), '\n\n\n')
    return dfxs     
        

def prep_dfs(dfs, cols_to_keep):
    '''
    The idea here is to make each dataframe the same as the dataframes used in the training of the model. 
    '''
    prepped_dfs = []
    for df in dfs:
        # Use a more flexible parser for the 'time' column
        df['time'] = df['time'].apply(lambda x: parser.parse(x))

        df['time'] = df['time'].dt.round('30min')
        df.drop_duplicates(subset='time', keep='last', inplace=True)
        df.sort_values(by='time', ascending=True, inplace=True)
        df.replace(999, np.nan, inplace=True)
        df.replace(99, np.nan, inplace=True)
        df.fillna(method='ffill', inplace=True)
        prepped_df = df[cols_to_keep].copy()
        setattr(prepped_df, 'name', getattr(df, 'name', 'default_name'))
        prepped_dfs.append(prepped_df)
        
    return prepped_dfs

def convert_columns_to_float(dfx):
    float_columns = [col for col in dfx.columns 
                     if col.lower() not in ['time'] and 'mwd' not in col.lower()]
    for column in float_columns:
        dfx[column] = pd.to_numeric(dfx[column], errors='coerce').astype(float)
    return dfx

def is_swell_now(df, column_name, window, variability_factor, min_jump, sustain_window):
    '''
    Determines if there's a swell in the latest row of the DataFrame based on specified criteria.
    
    Parameters:
    - df: DataFrame containing the data, sorted by time.
    - column_name: Name of the column to analyze for swell starts (e.g., 'DPD').
    - window: Number of previous values to consider for calculating mean and variability.
    - variability_factor: Multiplier for the standard deviation to set the threshold for detecting swells.
    - min_jump: Minimum increase over the mean required to consider a value as indicating a swell start.
    - sustain_window: Number of values following a detected swell start within which the swell conditions must be met again to confirm.
    
    Returns:
    - True if the latest row indicates a swell, False otherwise.
    '''
    latest_index = len(df) - 1
    if latest_index < window:  # Not enough data
        return False
    
    # Calculate mean and standard deviation for the window leading up to the latest row
    window_mean = df[column_name].iloc[-window:].mean()
    window_std = df[column_name].iloc[-window:].std()
    
    # Define the threshold for a significant jump
    threshold = window_mean + (window_std * variability_factor)
    
    # Check if the latest value exceeds the threshold and meets the minimum jump criteria
    if df[column_name].iloc[latest_index] > threshold and (df[column_name].iloc[latest_index] - window_mean) >= min_jump:
        # Check for sustainment within the sustain_window
        for j in range(1, min(sustain_window + 1, latest_index + 1)):
            if df[column_name].iloc[latest_index - j] - window_mean >= min_jump:
                return True
    return False
# result = is_swell_now(df, 'DPD', window_size, variability_factor, min_jump, sustain_window)

# Example usage with your DataFrame (assuming it's named df and sorted by 'time')

scraped_dfs = get_buoys(buoy_ids)
df205 = scraped_dfs[0]
# drop unneccessary columns
df205 = df205.loc[:, ['time', 'WVHT', 'DPD', 'MWD']]
# make DPD value a float
df205['DPD'] = pd.to_numeric(df205['DPD'], errors='coerce')
# sort the df from early to late
df205 = df205.sort_values(by='time', ascending=True)
swell_test = is_swell_now(df205, 'DPD', window_size, variability_factor, min_jump, sustain_window)
print(f"Swell right now: {swell_test}")


# prepped_dfs = prep_dfs(dfs, cols_to_keep)
# df = prepped_dfs[0]
# df = convert_columns_to_float(df)
# prepped_wswells = create_swell_feature(df, 'DPD', window_size, variability_factor, min_jump, sustain_window)
