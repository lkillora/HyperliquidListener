import requests
import json
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from scrapingbee import ScrapingBeeClient
import pandas as pd

load_dotenv('.env', override=True)
scraping_bee_api_key = os.environ.get('SCRAPING_BEE_API_KEY')

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





def scrape_with_playwright(symbol):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page(extra_http_headers=headers)
        page.goto(f"https://hyperdash.info/api/hyperdash/full/ticker_overview?symbol={symbol}")
        content = page.inner_text("body")
        print(content)


def scrape_hyperdash_with_scraping_bee_request(symbol='XPL'):
    hyperdash_url = f"https://hyperdash.info/api/hyperdash/full/ticker_overview?symbol={symbol}"
    params = {
        "api_key": scraping_bee_api_key,
        "url": hyperdash_url,
        "render_js": "true",
        "forward_headers": "true",
        "block_resources": "false"
    }
    response = requests.get("https://app.scrapingbee.com/api/v1/", params=params, headers=headers)
    if response.ok:
        print(response.json())
    else:
        print(response.status_code, response.text)



def fetch_all_perps():
    hyperliquid_url = 'https://api.hyperliquid.xyz/info'
    response = requests.post(hyperliquid_url, headers=headers, json={'type': 'meta'})
    tks = response.json()
    universe = tks['universe']
    perps = pd.DataFrame(universe).reset_index()
    perps = perps[perps['isDelisted'].isnull()].reset_index(drop=True)
    perps = perps[['index', 'name', 'maxLeverage', 'onlyIsolated']].rename(columns={'index': 'id'})
    return perps

def fetch_all_spot():
    response = requests.post(hyperliquid_url, headers=headers, json={'type': 'spotMetaAndAssetCtxs'})
    tks, prices = response.json()
    universe = tks['universe']
    tokens = tks['tokens']
    spot = [{'id': u['name'], 'name': f'{tokens[u['tokens'][0]]['name']}/{tokens[u['tokens'][1]]['name']}'} for u in universe]
    spot = pd.DataFrame(spot)
    interesting_spot_assets = [
        'UPUMP/USDC',
        'HYPE/USDC',
        'UETH/USDC',
        'UBTC/USDC',
        'USOL/USDC',
        'HPENGU/USDC'
    ]
    spot = spot[spot['name'].apply(lambda a: a in interesting_spot_assets)].reset_index(drop=True)
    spot['maxLeverage'] = 1
    spot['onlyIsolated'] = None
    spot = spot.rename(columns={'id': 'name', 'name': 'id'})
    spot = spot[['name', 'id']].set_index('name').to_dict()['id']
    print(json.dumps(spot, indent=4))
    return spot


def fetch_mids():
    hyperliquid_url = "https://api.hyperliquid.xyz/info"
    payload = {
        "type": "allMids"
    }
    response = requests.post(hyperliquid_url, json=payload)
    prices = pd.DataFrame.from_dict(response.json(), orient='index').reset_index()
    prices.columns = ['symbol', 'mid']
    prices = prices[prices['symbol'].apply(lambda s: s[0] != '@' or s in spot_assets)].reset_index(drop=True)
    prices.to_csv('./prices.csv', index=False)
