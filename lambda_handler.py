from utils import set_swell, get_ttl, check_for_swells, send_message_via_twilio, initialize_session
import os
import logging
import boto3
import uuid
import time

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
dynamodb = boto3.resource('dynamodb')

CURRENT_SWELLS_TABLE = dynamodb.Table('current_swells')
SWELL_ALERTS_TABLE = dynamodb.Table('SwellNotifications')
USERS_TABLE = dynamodb.Table('users')
PENDING_MESSAGES_TABLE = dynamodb.Table('pending_messages')
CCWIRELESS_PHONE = os.getenv('CCWIRELESS_PHONE')


SESSION_LENGTH = int(os.environ['SESSION_LENGTH'])
DIRECTION_MAP = {
    'N': 'North',
    'NNE': 'North-Northeast',
    'NE': 'Northeast',
    'ENE': 'East-Northeast',
    'E': 'East',
    'ESE': 'East-Southeast',
    'SE': 'Southeast',
    'SSE': 'South-Southeast',
    'S': 'South',
    'SSW': 'South-Southwest',
    'SW': 'Southwest',
    'WSW': 'West-Southwest',
    'W': 'West',
    'WNW': 'West-Northwest',
    'NW': 'Northwest',
    'NNW': 'North-Northwest'
}
SWELL_DEBOUNCE_TIME = int(os.environ['SWELL_DEBOUNCE_TIME']) #in hours
def lambda_handler(event=None, context=None):

    results = check_for_swells()

    # # to test
    # results[0]['swell'] = 1
    
    for result in results:
        if result['swell']:
            print(f"Swell detected for buoy {result['buoy_id']} with data: {result}")

            # IF THERE IS SWELL DETECTED, CHECK TO SEE IF buoy_id IS IN current_swells TABLE
            response = CURRENT_SWELLS_TABLE.query(KeyConditionExpression=Key('buoy_id').eq(result['buoy_id']))
            
            # IF IT IS NOT, ADD IT TO THE TABLE with ttl = SWELL_DEBOUNCE_TIME AND SEND A MESSAGE
            ttl = get_ttl(SWELL_DEBOUNCE_TIME)
            if response.get('Items'):
                logger.info(f"Swell already detected for buoy {result['buoy_id']}. Latest data: {result}")
                pass
            else:
                # set the ttl for the current_swells table
                set_swell(result['buoy_id'], ttl)

                swell_direction = DIRECTION_MAP.get(result['MWD'], result['MWD'])
                msg = f"{result['buoy_name']} buoy alert!\n\n {swell_direction} swell @ {result['WVHT']}ft & {result['DPD']}sec.\n\nhttps://www.ndbc.noaa.gov/station_page.php?station={result['buoy_id']}.\n\n Any questions? Just ask."
                
                # db_response = SWELL_TABLE.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('buoy_id').eq(result['buoy_id']))

                # # Extract phone numbers from the query response
                # phone_numbers = [item['phone_number'] for item in db_response.get('Items', [])]
                response = SWELL_ALERTS_TABLE.scan(FilterExpression=boto3.dynamodb.conditions.Attr('buoy_id').eq(result['buoy_id']))
                phone_numbers = [item['phone_number'] for item in response.get('Items', [])]

                # Send text messages to each phone number
                # Assuming you have a function send_text_message(phone_number, message) to send the messages
                for phone_number in phone_numbers:
                    session_id = initialize_session(phone_number, SESSION_LENGTH)
                    unique_id = send_message_via_twilio(phone_number, msg, session_id)
                

        # IF IT IS, DO NOTHING (PASS)
            
        else:
            print(f"No swell detected for buoy {result['buoy_id']}. Latest data: {result}")
    

    return {
        'statusCode': 200,
        'body': 'swell scan complete'
    }
