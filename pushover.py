
from dotenv import load_dotenv
import os
import http.client, urllib.parse
import aiohttp

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


async def async_send_pushover_alert(message, priority=0, user_key=pushover_user_key):
    if priority == 2:
        sound = "persistent"
    else:
        sound = "tugboat"

    payload = {
        "token": pushover_api_key,
        "user": user_key,
        "message": message,
        "priority": priority,
        "retry": 30,
        "expire": 600,
        "sound": sound,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.pushover.net/1/messages.json",
            data=urllib.parse.urlencode(payload),
            headers={"Content-type": "application/x-www-form-urlencoded"},
        ) as resp:
            result = await resp.text()
            print(result)
            return result