import time
import pandas as pd
import logging
from pushover import send_pushover_alert
import requests


spot_assets = {
    "@107": "HYPE/USDC",
    "@142": "UBTC/USDC",
    "@151": "UETH/USDC",
    "@156": "USOL/USDC",
    "@184": "HPENGU/USDC",
    "@188": "UPUMP/USDC"
}

def fetch_mids():
    hyperliquid_url = "https://api.hyperliquid.xyz/info"
    payload = {
        "type": "allMids"
    }
    response = requests.post(hyperliquid_url, json=payload)
    prices = pd.DataFrame.from_dict(response.json(), orient='index').reset_index()
    prices.columns = ['symbol', 'mid']
    prices = prices[prices['symbol'].apply(lambda s: s[0] != '@' or s in spot_assets)].reset_index(drop=True)
    prices.to_csv('./key_stats/prices.csv', index=False)
    return prices


def fetch_prices():
    logging.basicConfig(
        filename='./logs/prices.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    try:
        logging.info(f'Fetching mids')
        mids = fetch_mids()
        logging.info(f'Fetched {mids.shape[0]} mids')
        time.sleep(5)

    except Exception as e:
        msg = f'Prices call failed because {e}'
        logging.error(msg)
        send_pushover_alert(msg, priority=-1)
        time.sleep(60)


if __name__ == '__main__':
    while True:
        fetch_prices()


