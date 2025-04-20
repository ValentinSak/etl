import requests

def send_message(recipient: dict, message: dict) -> None:
    if recipient['type_channel'] == 'channel':
        send_message_to_channel(recipient['url'], message)
    elif recipient['type_channel'] == 'direct_message':
        send_message_with_bot(recipient['token'], recipient['channel'], message)
    else:
        raise ValueError(f'Recipient type_channel does not exist')
    print(f'Message has been successfuly sent to {recipient["id"]}')


def send_message_to_channel(webhook_url: str, message: dict):
    response = requests.post(webhook_url, json=message)
    if response.ok:
        print('Message send successfuly')
    else:
        print(f'failed to send message, status_code: {response.status_code}, reason: {response.reason}, text: {response.text}')
        raise ValueError(f'Message was not send')
    

def send_message_with_bot(token: str, channel: str, message: dict) -> None:
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
