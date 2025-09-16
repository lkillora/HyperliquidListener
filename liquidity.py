import time
import json
import pandas as pd
import ccxt
import logging
from pushover import send_pushover_alert
from glob import glob
import math

spot_assets = {
    "@107": "HYPE/USDC",
    "@142": "UBTC/USDC",
    "@151": "UETH/USDC",
    "@156": "USOL/USDC",
    "@184": "HPENGU/USDC",
    "@188": "UPUMP/USDC"
}

exchange = ccxt.hyperliquid()


def convert_to_asset(row):
    if not row['spot']:
        return row['baseName']
    if row['id'] == 'PURR/USDC':
        return 'PURR'
    return row['id']


def estimate_liq(bins, mid, threshold=0.05, is_bid=True):
    bins['usd'] = bins['px'] * bins['sz']
    bins['cumliq'] = bins['usd'].cumsum()
    bins['delta'] = (mid - bins['px'])*(is_bid*2 - 1)/mid
    under_threshold = bins[bins['delta'] <= threshold]
    over_threshold = bins[bins['delta'] > threshold]
    if over_threshold.empty:
        liq = float(bins['cumliq'].max())
        delta = float(bins['delta'].max())
        liq *= threshold/delta
    elif under_threshold.empty:
        liq = float(bins['cumliq'].min())
        delta = float(bins['delta'].min())
        liq *= delta/threshold
    else:
        over = over_threshold.iloc[0]
        under = under_threshold.iloc[-1]
        over_liq = over['cumliq']
        under_liq = under['cumliq']
        over_dist = abs(over['delta'] - threshold)
        under_dist = abs(under['delta'] - threshold)
        over_weight = under_dist / (under_dist + over_dist)
        under_weight = over_dist / (under_dist + over_dist)
        liq = float(over_liq * over_weight + under_liq * under_weight)
    liq = round(liq/1e3)
    return liq


def fetch_assets():
    markets = exchange.fetch_markets()
    assets = []
    for m in markets:
        if float(m['info']['dayNtlVlm']) >= 250_000:
            asset = {
                'id': m['id'],
                'symbol': m['symbol'],
                'baseName': m['baseName'],
                'spot': m['spot'],
                'midPx': m['info']['midPx'],
                'maxLeverage': m['info']['maxLeverage'] if not m['spot'] else None,
                'openInterest_mil': round(float(m['info']['openInterest'])*float(m['info']['midPx'])/1e6, 3) if not m['spot'] else None,
                'totalSupplyMcap_bil': round(float(m['info']['totalSupply']) * float(m['info']['markPx'])/1e9, 3) if m['spot'] else None,
                'dayNtlVlm_mil': round(float(m['info']['dayNtlVlm'])/1e6, 3)
            }
            assets.append(asset)
    assets = pd.DataFrame(assets)
    assets = assets[assets['baseName'] != 'PURR'].reset_index(drop=True)
    assets['asset'] = assets.apply(lambda row: convert_to_asset(row), axis=1)
    assets.to_csv('./key_stats/assets.csv', index=False)
    return assets


def refresh_liquidity():
    liq_files = glob('./liquidity/*.json')
    all_liquidity = []
    for f in liq_files:
        with open(f, 'r') as file:
            all_liquidity.append(json.load(file))
    all_liquidity = pd.DataFrame(all_liquidity)
    all_liquidity.to_csv('./key_stats/all_liquidity.csv', index=False)


def fetch_liquidity():
    logging.basicConfig(
        filename='./logs/liquidity.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    asset = 'PLACEHOLDER'
    try:
        assets = fetch_assets()
        logging.info(f'Fetched {assets.shape[0]} assets')
        all_liquidity = []
        for i, row in assets.iterrows():
            try:
                symbol = row['symbol']
                asset = row['asset']
                logging.info(f'Fetching orderbook for {asset} or {symbol}')
                order_book = exchange.fetch_order_book(symbol, limit=None, params={"nSigFigs": 2})
                logging.info(f'Orderbook with {len(order_book['bids'])} levels for {symbol}')
                bids = pd.DataFrame(order_book['bids'], columns=['px', 'sz'])
                asks = pd.DataFrame(order_book['asks'], columns=['px', 'sz'])
                mid = float(bids.iloc[0]['px'] + asks.iloc[0]['px'])*0.5
                liquidity = {
                    'mid': mid,
                    'bid_5': estimate_liq(bids, mid, threshold=0.05, is_bid=True),
                    'bid_10': estimate_liq(bids, mid, threshold=0.05, is_bid=True),
                    'ask_5': estimate_liq(asks, mid, threshold=0.1, is_bid=False),
                    'ask_10': estimate_liq(asks, mid, threshold=0.1, is_bid=False),
                }
                liquidity.update(row)
                all_liquidity.append(liquidity)
                with open(f'./liquidity/{asset}.json', 'w') as f:
                    json.dump(liquidity, f, indent=4)
                time.sleep(2.5)

            except Exception as e:
                msg = f'Liquidity call failed on {asset} because of {e}'
                logging.error(msg)
                send_pushover_alert(msg, priority=-1)
                time.sleep(60)
        all_liquidity = pd.DataFrame(all_liquidity)
        all_liquidity.to_csv(f'./key_stats/all_liquidity.csv', index=False)

    except Exception as e:
        msg = f'Liquidity call failed on {asset} because of {e}'
        logging.error(msg)
        send_pushover_alert(msg, priority=-1)
        time.sleep(60)


if __name__ == '__main__':
    while True:
        fetch_liquidity()


