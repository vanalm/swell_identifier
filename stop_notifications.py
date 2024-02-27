def stop_notifications(phone_number, message_body, session_id):
    '''
    fill this docstring out please 
    '''

    try:
        # If the message is just 'stop', remove all notifications for the user
        if message_body.lower() == 'stop':
            # Scan for all items with the matching phone number
            response = DAILY_TABLE.scan(
                FilterExpression=Key('phone_number').eq(phone_number)
            )
            items = response.get('Items', [])
            for item in items:
                table.delete_item(
                    Key={
                        'phone_number': phone_number,
                        'topic': item['topic']
                    }
                )

            msg = "You are unsubscribed!\n\n Feel free to opt back in anytime, or just ask any question you may have directly. \n\nMahalo!"
            send_message_via_twilio(phone_number, msg, session_id)
            return f"All notifications stopped for {phone_number}."

        # If the message includes a topic, remove only that specific notification
        elif len(message_body.lower().split()) == 2 and 'stop' in message_body.lower().split():
            if 'alerts' in message_body.lower().split():
                try:
                    # Query to get all items for the phone_number
                    response = SWELL_TABLE.query(
                        KeyConditionExpression=Key('phone_number').eq(phone_number)
                    )
                    
                    # Iterate over the items and delete each one
                    for item in response['Items']:
                        SWELL_TABLE.delete_item(
                            Key={
                                'phone_number': item['phone_number'],
                                'sort_key_attribute_name': item['sort_key_attribute_name']  # Replace 'sort_key_attribute_name' with your actual sort key attribute name
                            }
                        )
                    except Exception as e:
                    print(f'problem removing user from SwellNotifications: {e}')
                    
                
            elif message_body.lower().split()[0] == 'stop':
                topic = message_body.lower().split()[1]
            elif message_body.lower().split()[1] == 'stop':
                topic = message_body.lower().split()[0]
            try:
                DAILY_TABLE.delete_item(
                    Key={
                        'phone_number': phone_number,
                        'topic': topic
                    }
                )
            except Exception as e:
                print(f'Error removing user from DAILY_TABLE: {e}')
                
            msg = f"But how will you keep up with {topic} conditions?!\njk... you're unsubscribed! Opt back in at any time, or feel free to just ask a question directly! \nMahalo!"
            send_message_via_twilio(phone_number, msg, None)
            return f"Notifications for {topic} stopped for {phone_number}."

        else:
            return "Invalid request format."
            msg = "something went wrong... please contact jake@cocowireless.org"
            send_message_via_twilio(phone_number, msg, None)
    except Exception as e:
        msg = "something went wrong... please contact jake@cocowireless.org"
        send_message_via_twilio(phone_number, msg, None)
        return f"Error removing user from notifications: {str(e)}"