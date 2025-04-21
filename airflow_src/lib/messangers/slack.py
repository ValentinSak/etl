import requests


def send_message(recipient: dict, message: dict) -> None:
    """Sends a message to a Slack recipient using appropriate method based on recipient type
    
    params:
    recipient - dictionary containing recipient information:
        - type_channel: 'channel' or 'direct_message'
        - url: webhook url (for channel)
        - token: bot token (for direct_message)
        - channel: channel name/id (for direct_message)
        - id: recipient identifier
    message - dictionary containing the message to send
    
    Raises ValueError if recipient type is not supported
    """
    if recipient['type_channel'] == 'channel':
        send_message_to_channel(recipient['url'], message)
    elif recipient['type_channel'] == 'direct_message':
        send_message_with_bot(recipient['token'], recipient['channel'], message)
    else:
        raise ValueError(f'Recipient type_channel does not exist')
    print(f'Message has been successfuly sent to {recipient["id"]}')


def send_message_to_channel(webhook_url: str, message: dict) -> None:
    """Sends a message to a Slack channel using webhook
    
    params:
    webhook_url - Slack webhook url for the channel
    message - dictionary containing the message to send
    
    Raises ValueError if message sending fails
    """
    response = requests.post(webhook_url, json=message)
    if response.ok:
        print('Message send successfuly')
    else:
        print(f'failed to send message, status_code: {response.status_code}, reason: {response.reason}, text: {response.text}')
        raise ValueError(f'Message was not send')
    

def send_message_with_bot(token: str, channel: str, message: dict) -> None:
    """Sends a direct message using Slack bot token
    
    params:
    token - Slack bot authentication token
    channel - channel name or id to send message to
    message - dictionary containing the message to send
    
    Raises ValueError if message sending fails
    """
    url = 'https://salck.com/api/chat.postMessage'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    message['channel'] = channel
    response = requests.post(url, headers=headers, json=message)
    if response.ok:
        print('Message send successfuly')
    else:
        print(f'failed to send message, status_code: {response.status_code}, reason: {response.reason}, text: {response.text}')
        raise ValueError(f'Message was not send')
    
def get_message(header: str, message: str) -> dict:
    """Creates a formatted Slack message with header and content
    
    params:
    header - text to display as message header
    message - main message content
    
    Returns a dictionary with Slack message blocks format:
    - header block with provided header text
    - divider
    - section block with markdown-formatted message
    """
    slack_message = {
        'blocks': [
            {
                'type': 'header',
                'text': {
                    'type': 'plain_text',
                    'text': f'{header}'
                }
            },
            {'type': 'divider'},
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdown',
                    'text': f'{message}'
                },
            },
            
        ]
    }
    
    return slack_message
