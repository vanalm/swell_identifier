import requests
from lxml import html
import pandas as pd

# Define buoy IDs and map
buoy_ids = ['51101', '51208', '51201', '51210', '51205', '51206']
buoy_map = {
    '51101': '186 NM NW of Kauai',
    '51208': 'Hanalei',
    '51201': 'Waimea Bay',
    '51210': 'Kaneohe Bay',
    '51205': 'Pauwela',
    '51206': 'Hilo'
}
cols_to_keep = ['time', 'WVHT', 'DPD', 'MWD']

# Swell detection parameters
window_size = 3
variability_factor = 3
min_jump = 2
sustain_window = 2


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
    return df.loc[:, cols_to_keep]

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
        'buoy_name': buoy_map.get(buoy_id, "Unknown buoy"),
        'swell': swell_detected, 
        **latest_readings
    }
    return result

def is_swell_now(df, column_name, window, variability_factor, min_jump, sustain_window):
    latest_index = len(df) - 1
    window_mean = df[column_name].iloc[-window:].mean()
    window_std = df[column_name].iloc[-window:].std()
    threshold = window_mean + (window_std * variability_factor)
    
    swell_detected = 0
    if df[column_name].iloc[latest_index] > threshold and (df[column_name].iloc[latest_index] - window_mean) >= min_jump:
        for j in range(1, sustain_window + 1):
            if df[column_name].iloc[latest_index - j] - window_mean >= min_jump:
                swell_detected = 1
                break
    
    latest_readings = {
        'time': df['time'].iloc[-1],  # Keep as string
        'WVHT': df['WVHT'].iloc[-1],
        'DPD': df['DPD'].iloc[-1],
        'MWD': df['MWD'].iloc[-1]  # Keep as string
    }
    
    return swell_detected, latest_readings

results = []
for buoy_id in buoy_ids:
    result = prepare_and_analyze_data(buoy_id)
    if result:
        results.append(result)

for result in results:
    if result['swell']:
        print(f"Swell detected for buoy {result['buoy_id']} with data: {result}")
    else:
        print(f"No swell detected for buoy {result['buoy_id']}. Latest data: {result}")

