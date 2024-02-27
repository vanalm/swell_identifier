import requests
from lxml import html
import pandas as pd
from datetime import timedelta, datetime
from dateutil import parser
import time
from twilio.rest import Client
from twilio.twiml.messaging_response import MessagingResponse
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import boto3
import os
import logging
import datetime
import uuid

TWILIO_SID = os.getenv('TWILIO_SID')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN')
MESSAGING_SERVICE_SID = os.getenv('MESSAGING_SERVICE_SID')

cols_to_keep = ['time', 'WVHT', 'DPD', 'MWD']

window_size = 3
variability_factor = 3  # Adjust based on further analysis of the data
min_jump = 2  # Minimum jump to consider as a swell start
sustain_window = 2  # Number of values to confirm the swell condition is sustained
dynamodb = boto3.resource('dynamodb')
SESSION_LENGTH = int(os.environ['SESSION_LENGTH'])

SESSIONS_TABLE = dynamodb.Table('sessions')
swell_table = dynamodb.Table('SwellNotifications')
users_table = dynamodb.Table('users')

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
PENDING_MESSAGES_TABLE = dynamodb.Table('pending_messages')

# Define buoy IDs and map
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
    return df.loc[:, COLS_TO_KEEP]

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

def subscribe_user_to_swells(phone_number, buoy_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UserBuoyPreferences')
    
    table.put_item(
        Item={
            'phone_number': phone_number,
            'buoy_id': buoy_id
        }
    )
    
    print(f"Subscription added for {phone_number} to buoy {buoy_id}")

def unsubscribe_user_from_swells(phone_number, buoy_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UserBuoyPreferences')
    
    table.delete_item(
        Key={
            'phone_number': phone_number,
            'buoy_id': buoy_id
        }
    )
    
    print(f"Subscription removed for {phone_number} from buoy {buoy_id}")

def send_message_via_twilio(phone_number, message_body, session_id):
    """
    Send a message via the Twilio API and saves relevant details for status callback and session.

    Parameters:
        phone_number (str): The recipient's phone number.
        message_body (str): The body of the message to be sent.

    Returns:
        str: The unique_id used for identifying the message or None if sending fails.
    """
    unique_id = str(uuid.uuid4())

    try:
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        message = client.messages.create(
            messaging_service_sid=MESSAGING_SERVICE_SID,
            # from_=CCWIRELESS_PHONE,
            body=message_body,
            to=phone_number,
            status_callback=f'https://3ia7ku3dozymdbodomst2ht6ny0cdpuo.lambda-url.us-west-2.on.aws/?unique_id={unique_id}'
        )
        print(' see mee'*10)
        logging.info(f'Sent message: {message_body} to {phone_number} with UUID {unique_id}')
    except Exception as e:
        logging.error(f"Error sending message via Twilio API: {e}")
        return None

    try:
        # here I need to save to pending instead of calling these functions.
        # cache_for_callback(unique_id, message_body, 'assistant', phone_number)
        # cache_message(session_key, "assistant", message_body, MAX_MESSAGES_PER_SESSION)
        save_response_to_pending(session_id, phone_number, message_body, unique_id)
    except Exception as e:
        logging.error(f"Error storing message in user session: {e}")

    return unique_id
    
def initialize_session(phone_number, SESSION_LENGTH=SESSION_LENGTH):
    """
    Initializes a session for a user with a Time To Live (TTL) in the 'sessions' table.
    Uses the global SESSIONS_TABLE variable.

    Args:
    - phone_number (str): The user's phone number.
    - session_duration (int): Duration of the session in seconds.

    Returns:
    str: The session ID if successful, None otherwise.
    """
    try:
        print('initializing session...')
        # Generate a new unique session_id
        session_id = str(uuid.uuid4())
        # Current time in epoch seconds
        current_time = int(time.time())
        
        # Set the TTL (expire time) for the session
        ttl = current_time + SESSION_LENGTH

        # Create a new session with TTL in the 'sessions' table
        SESSIONS_TABLE.put_item(
            Item={
                'phone_number': phone_number,
                'session_id': session_id,
                'start_time': current_time,
                'msg_count': 1,
                'ttl': ttl
            }
        )
        return session_id
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None
        

def save_response_to_pending(session_id, phone_number, message_body, message_id):
    """
    Saves the message to the 'pending_messages' table with a 'pending' status.

    Args:
    - session_id (str): The session ID associated with the user's ongoing session.
    - phone_number (str): The user's phone number.
    - message_body (str): The content of the message to be saved.

    Returns:
    str: The unique identifier of the pending message or None if an error occurs.
    """
    # Generate a unique identifier for the message
    # message_id = str(uuid.uuid4())
    # print(f"message_id = {message_id}")
    # current time
    # current_time = int(time.time())                #JUST CHANGED THIS ON THANKSGIVING
    
    # Create a dictionary for the pending message
    pending_message = {
        'message_id': message_id,
        'session_id': session_id,
        'phone_number': phone_number,
        'content': message_body,
        'timestamp': get_current_time(),
        'status': 'pending'
    }

    # Try to save the pending message to the DynamoDB table
    try:
        PENDING_MESSAGES_TABLE.put_item(Item=pending_message)
        logger.info(f"Pending message saved with ID: {message_id}")
        return None
    except Exception as e:
        logger.error(f"Failed to save pending message: {e}")
        return None


def get_current_time(format="%Y-%m-%d %H:%M:%S"):
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime(format)
    return str(formatted_time)
    
def subscribe_user_to_swells(phone_number, buoy_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UserBuoyPreferences')
    
    table.put_item(
        Item={
            'phone_number': phone_number,
            'buoy_id': buoy_id
        }
    )
    
    print(f"Subscription added for {phone_number} to buoy {buoy_id}")

def unsubscribe_user_from_swells(phone_number, buoy_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UserBuoyPreferences')
    
    table.delete_item(
        Key={
            'phone_number': phone_number,
            'buoy_id': buoy_id
        }
    )
    
    print(f"Subscription removed for {phone_number} from buoy {buoy_id}")
