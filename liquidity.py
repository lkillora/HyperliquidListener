import time
import requests
import json
import os
from dotenv import load_dotenv
from scrapingbee import ScrapingBeeClient
import pandas as pd
import ccxt

load_dotenv('.env', override=True)
scraping_bee_api_key = os.environ.get('SCRAPING_BEE_API_KEY')
exchange = ccxt.hyperliquid()

def convert_to_asset(row):
    if not row['spot']:
        return row['baseName']
    if row['id'] == 'PURR/USDC':
        return 'PURR'
    return row['id']

spot_assets = {
    "@107": "HYPE/USDC",
    "@142": "UBTC/USDC",
    "@151": "UETH/USDC",
    "@156": "USOL/USDC",
    "@184": "HPENGU/USDC",
    "@188": "UPUMP/USDC"
}


def estimate_liq(bins, mid, threshold=0.05, is_bid=True):
    bins['usd'] = bins['px'] * bins['sz']
    bins['cumliq'] = bins['usd'].cumsum()
    bins['delta'] = (mid - bins['px'])*(is_bid*2 - 1)/mid
    under_threshold = bins[bins['delta'] <= threshold]
    over_threshold = bins[bins['delta'] > threshold]
    if over_threshold.empty:
        liq = float(bins['cumliq'].max())
    elif under_threshold.empty:
        liq = float(bins['cumliq'].min())
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
                'openInterest_mil': round(float(m['info']['openInterest'])/1e6, 3) if not m['spot'] else None,
                'totalSupplyMcap_bil': round(float(m['info']['totalSupply']) * float(m['info']['markPx'])/1e9, 3) if m['spot'] else None,
                'dayNtlVlm_mil': round(float(m['info']['dayNtlVlm'])/1e6, 3)
            }
            assets.append(asset)
    assets = pd.DataFrame(assets)
    assets = assets[assets['baseName'] != 'PURR'].reset_index(drop=True)
    assets['asset'] = assets.apply(lambda row: convert_to_asset(row), axis=1)
    assets.to_csv('./assets.csv', index=False)
    return assets


def fetch_liquidity():
    assets = fetch_assets()
    for i, row in assets.iterrows():
        symbol = row['symbol']
        asset = row['asset']
        order_book = exchange.fetch_order_book(symbol, limit=None, params={"nSigFigs": 3})
        bids = pd.DataFrame(order_book['bids'], columns=['px', 'sz'])
        asks = pd.DataFrame(order_book['asks'], columns=['px', 'sz'])
        mid = float(bids.iloc[0]['px'] + asks.iloc[0]['px'])*0.5
        liquidity = {
            'mid': mid,
            'bid_5': estimate_liq(bids, mid, threshold=0.05, is_bid=True),
            'bid_10': estimate_liq(bids, mid, threshold=0.05, is_bid=True),
            'ask_5': estimate_liq(asks, mid, threshold=0.05, is_bid=False),
            'ask_10': estimate_liq(asks, mid, threshold=0.05, is_bid=False),
        }
        liquidity.update(row)
        with open(f'./liquidity/{asset}.json', 'w') as f:
            json.dump(liquidity, f, indent=4)
        time.sleep(1)

