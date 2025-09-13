import logging
import time
import json
import os
from dotenv import load_dotenv
from scrapingbee import ScrapingBeeClient
import pandas as pd
import ccxt
from pushover import send_pushover_alert

load_dotenv('.env', override=True)
scraping_bee_api_key = os.environ.get('SCRAPING_BEE_API_KEY')
exchange = ccxt.hyperliquid()

spot_assets = {
    "@107": "HYPE/USDC",
    "@142": "UBTC/USDC",
    "@151": "UETH/USDC",
    "@156": "USOL/USDC",
    "@184": "HPENGU/USDC",
    "@188": "UPUMP/USDC"
}

headers = {
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "if-none-match": "W/\"jjdmko317e2lhh6\"",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-arch": "\"x86\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-full-version": "\"140.0.7339.128\"",
    "sec-ch-ua-full-version-list": "\"Chromium\";v=\"140.0.7339.128\", \"Not=A?Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"140.0.7339.128\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": "\"\"",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-ch-ua-platform-version": "\"19.0.0\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-api-key": "hyperdash_public_7vN3mK8pQ4wX2cL9hF5tR1bY6gS0jD",
    "cookie": "__client_uat=0; __client_uat_qNzK0IVd=0; cf_clearance=lqLmr_Ht3h.U..ps.7T8ZskyzRFSN5.arG7drNizTV8-1756415089-1.2.1.1-aLN0MIrNUKA5q8VMGSVarH5UCH9cne8kXtsX2NxwMJhKlxbiLycIR5O3THl0oE5O6e5TL2Z0btZf1xw_x5tytbkuHQFt5xgZwUiVeApIO1OWwCvlJvyOA1oGtGqsBWSkzp6Rpo_7pP._ZO4yLTaukqrw.GGBOmIJczAbYuyo4GUVnDqIX5TFnh7e6RS2JwNlk9JrVOLWznKHhIMXNnjhmd9DO0HNBk_mTYmxzh6FHPk",
    "Referer": "https://hyperdash.info/terminal"
}


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
    assets['asset'] = assets.apply(lambda row: row['baseName'] if not row['spot'] else row['id'], axis=1)
    assets = assets[~assets['asset'].str.contains('PURR')].reset_index(drop=True)
    assets.to_csv('./key_stats/assets.csv', index=False)
    return assets


def scrape_hyperdash_with_scraping_bee_sdk(symbol='XPL'):
    if '@' in symbol:
        ticker = spot_assets[symbol].replace('/USDC', '')
        hyperdash_url = f"https://hyperdash.info/api/hyperdash/full/spot_overview?symbol={ticker}"
    else:
        hyperdash_url = f"https://hyperdash.info/api/hyperdash/full/ticker_overview?symbol={symbol}"
    client = ScrapingBeeClient(api_key=scraping_bee_api_key)
    response = client.get(hyperdash_url, headers=headers)
    data = json.loads(response.content)
    with open(f'./positions/{symbol}.json', "w") as f:
        json.dump(data, f, indent=4)
    return data


def fetch_positions():
    logging.basicConfig(
        filename='./logs/positions.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    asset = 'PLACEHOLDER'
    try:
        assets = fetch_assets()
        logging.info(f'Fetched {assets.shape[0]} assets')
        for i, row in assets.iterrows():
            try:
                asset = row['asset']
                logging.info(f'Fetching positions for {asset}')
                positions = scrape_hyperdash_with_scraping_bee_sdk(asset)
                if 'positions' in positions:
                    logging.info(f'Fetched {len(positions['positions'])} positions for {asset}')
                else:
                    logging.info(f'Fetched 0 positions for {asset}')
                time.sleep(10)
            except Exception as e:
                msg = f'Positions call failed for {asset} because of {e}'
                logging.error(msg)
                send_pushover_alert(msg, priority=-1)
                time.sleep(60)
    except Exception as e:
        msg = f'Positions call failed for {asset} because of {e}'
        logging.error(msg)
        send_pushover_alert(msg, priority=-1)
        time.sleep(60)


if __name__ == '__main__':
    while True:
        fetch_positions()

