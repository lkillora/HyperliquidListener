import asyncio
import json
import websockets
import os
import pandas as pd
from dotenv import load_dotenv
from pushover import async_send_pushover_alert

load_dotenv('.env', override=True)
api_key = os.environ.get('HYDROMANCER_API_KEY')
HYDROMANCER_WS_URL = f"wss://api.hydromancer.xyz/ws?token={api_key}"
RECONNECT_DELAY = 5

thresholds = {}
thresholds_lock = asyncio.Lock()
prices = {}
prices_lock = asyncio.Lock()


async def load_thresholds(filepath="./key_stats/all_liquidity.csv"):
    try:
        df = pd.read_csv(filepath)
        new_thresholds = {
            row["asset"]: {
                "spot": row['spot'],
                "liq_threshold": min(row["bid_5"], row["ask_5"]),
                "oi_threshold": row["openInterest_mil"] * 0.02 if row["openInterest_mil"] is not None else 0,
                'dv_threshold': row['dayNtlVlm_mil'] * 0.02
            }
            for _, row in df.iterrows()
        }
        async with thresholds_lock:
            thresholds.clear()
            thresholds.update(new_thresholds)
        print(f"Loaded thresholds")
    except FileNotFoundError:
        print(f"{filepath} not found. Skipping...")
    except Exception as e:
        print(f"Error reading {filepath}: {e}")


async def load_prices(filepath="./key_stats/prices.csv"):
    try:
        df = pd.read_csv(filepath)
        new_prices = {
            row["symbol"]: row['mid']
            for _, row in df.iterrows()
        }
        async with prices_lock:
            prices.clear()
            prices.update(new_prices)
        print(f"Loaded prices")
    except FileNotFoundError:
        print(f"{filepath} not found. Skipping...")
    except Exception as e:
        print(f"Error reading {filepath}: {e}")


async def watch_thresholds(thresholds_filepath="./key_stats/all_liquidity.csv", interval=60*30):
    while True:
        await load_thresholds(thresholds_filepath)
        await asyncio.sleep(interval)



async def watch_prices(prices_filepath="./key_stats/prices.csv", interval=60):
    while True:
        await load_prices(prices_filepath)
        await asyncio.sleep(interval)


async def filter_message(data):
    updates = [] if 'updates' not in data else data['updates']
    for u in updates:
        coin = u['coin']
        if coin in thresholds and coin in prices:
            stats = thresholds[coin]
            mid = prices[coin]
            minutes = float(u['minutes'])
            notional = float(u['sz']) * mid
            notional_check = notional > 250_000
            if notional_check:
                notional_30min = notional * min(1, 30 / minutes)
                liq_check = notional_30min/1e3 > stats['liq_threshold']
                oi_check = False if stats['spot'] else notional/1e6 > stats['oi_threshold']*0.01
                vol_check = False if not stats['spot'] else notional/1e6 > stats['dv_threshold']*0.01
                u['notional'] = notional
                u['notional_30min'] = notional_30min
                u['mid'] = mid
                u['liq'] = stats['liq_threshold']
                u['oi'] = stats['oi_threshold']
                u['dv'] = stats['dv_threshold']
                u['liq_check'] = liq_check
                u['oi_check'] = oi_check
                u['vol_check'] = vol_check
                if liq_check or oi_check or vol_check:
                    asyncio.create_task(async_send_pushover_alert(u, priority=1))


async def listen(ws):
    while True:
        try:
            message = await ws.recv()  # No timeout; ping handled automatically
            data = json.loads(message)
            print(data)
            await filter_message(data)

        except websockets.ConnectionClosed:
            print("Connection closed, reconnecting...")
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")


async def connect():
    while True:
        try:
            async with websockets.connect(
                HYDROMANCER_WS_URL,
                ping_interval=30,  # automatically ping every 30s
                ping_timeout=10    # wait up to 10s for pong
            ) as ws:
                print("Connected to Hydromancer WebSocket.")

                subscribe_message = {
                    "type": "subscribe",
                    "subscription": {"type": "allTwapStatusUpdates"},
                }
                await ws.send(json.dumps(subscribe_message))

                await listen(ws)

        except Exception as e:
            print(f"WebSocket error: {e}. Reconnecting in {RECONNECT_DELAY} seconds...")
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    await asyncio.gather(
        connect(),
        watch_thresholds("./key_stats/all_liquidity.csv", interval=1800),
        watch_prices("./key_stats/prices.csv", interval=60)
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")