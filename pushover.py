
from dotenv import load_dotenv
import os
import http.client, urllib.parse


load_dotenv(".env", override=True)
pushover_api_key = os.environ['MY_PUSHOVER_API_KEY']
pushover_user_key = os.environ['MY_WORK_PUSHOVER_USER_KEY']

for folder in ('positions', 'liquidity', 'key_stats', 'large_positions', 'liquidation_risks', 'logs'):
    os.makedirs(folder, exist_ok=True)

def send_pushover_alert(message, priority=0, user_key=pushover_user_key):
    if priority == 2:
        sound = "persistent"
    else:
        sound = "tugboat"

    conn = http.client.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
                 urllib.parse.urlencode({
                     "token": pushover_api_key,
                     "user": user_key,
                     "message": message,
                     "priority": priority,
                     "retry": 30,
                     "expire": 600,
                     "sound": sound,
                 }), {"Content-type": "application/x-www-form-urlencoded"})
    print(conn.getresponse().read())
    return None