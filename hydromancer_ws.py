import asyncio
import json
import websockets
import os
from dotenv import load_dotenv

load_dotenv('.env', override=True)
api_key = os.environ.get('HYDROMANCER_API_KEY')
HYDROMANCER_WS_URL = f"wss://api.hydromancer.xyz/ws?token={api_key}"
RECONNECT_DELAY = 5  # seconds


async def connect():
    while True:
        try:
            async with websockets.connect(HYDROMANCER_WS_URL) as ws:
                print("Connected to Hydromancer WebSocket.")

                # Subscribe to all fills (you can add 'dex' if needed)
                subscribe_message = {
                    "type": "subscribe",
                    "subscription": {
                        "type": "allTwapStatusUpdates"
                        # "dex": "dex_name"  # optional
                    }
                }
                await ws.send(json.dumps(subscribe_message))

                await listen(ws)
        except Exception as e:
            print(f"WebSocket error: {e}. Reconnecting in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)


async def listen(ws):
    while True:
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=60)
            data = json.loads(message)
            print(data)
        except asyncio.TimeoutError:
            print("No message received in 60s, sending ping...")
            try:
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=10)
            except Exception:
                print("Ping failed, reconnecting...")
                raise
        except websockets.ConnectionClosed:
            print("Connection closed, reconnecting...")
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(connect())
