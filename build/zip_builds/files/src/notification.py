"""
notification files
"""

import requests
import os

def notify(text_string):
    """
    primary function for notification
    If we add or change notification avenues, make the invocation change here
    """

    notify_discord_bot(text_string=text_string)


def notify_discord_bot(text_string,webhook=None):
    """
    notifies the discord webhook
    """

    DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK')
    
    data = {
        "content": str(text_string)
    }
    response = requests.post(url=DISCORD_WEBHOOK, json=data)
    print(response)
    
    print(f"notification complete")