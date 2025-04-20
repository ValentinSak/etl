configs = {
        'shared_dir_path': '/shared',
        'recipients': [
            {'id': 1, 'type_channel': 'channel', 'url': 'some_slack_webhook', 'name': 'some_name'},
            {'id': 2, 'type_channel': 'direct_message', 'token': 'some_token', 'channel': 'slack_id'},
        ],
        'thresholds': {'store_sales_alert': -20}
    }