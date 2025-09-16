import time
from glob import glob
import json
import os
from pushover import send_pushover_alert
import logging

alerted_positions = []

def summarise():
    position_files = glob('./positions/*.json')
    liquidity_files = glob('./liquidity/*.json')
    assets_with_positions = set([os.path.basename(f).replace('.json', '') for f in position_files])
    assets_with_liquidity = set([os.path.basename(f).replace('.json', '') for f in liquidity_files])
    overlap = list(assets_with_positions.intersection(assets_with_liquidity))

    top_hype_positions = []
    if '@107' in overlap:
        with open(f"./positions/@107.json", 'r') as f:
            hype_positions = json.load(f)
        with open(f"./liquidity/@107.json", 'r') as f:
            hype_liquidity = json.load(f)
        for h in hype_positions:
            if 'staked' in h:
                staked = h['staked']
            else:
                staked = 0
            notional = round((h['total'] - staked)*float(hype_liquidity['midPx'])/1e3)
            if  notional > hype_liquidity['bid_5']:
                h['notional'] = notional
                h['bid_5'] = hype_liquidity['bid_5']
                top_hype_positions.append(h)
        with open(f'./key_stats/top_hype_positions.json', "w") as f:
            json.dump(top_hype_positions, f, indent=4)

    top_hype_holders_upnl = {p['user']: 0 for p in top_hype_positions}
    overlap = [a for a in overlap if '@' not in a]
    for asset in overlap:
        with open(f"./positions/{asset}.json", 'r') as f:
            positions = json.load(f)
        with open(f"./liquidity/{asset}.json", 'r') as f:
            liquidity = json.load(f)

        if 'positions' in positions:
            liquidation_risks = []
            large_positions = []
            notionals = []
            accs = list(positions['positions'].keys())
            for acc in accs:
                position = positions['positions'][acc]
                notionals.append(position['notional_size'])
                position['dir'] = 'LONG' if position['size'] >= 0 else 'SHORT'
                position['liq'] = liquidity['bid_5'] if position['dir'] == 'SHORT' else liquidity['ask_5']
                position['mid'] = liquidity['mid']
                position['distance'] = round(abs(position['liquidation_price'] - position['mid'])/position['mid'], 2) if position['liquidation_price'] is not None else None
                position['address'] = acc
                position['asset'] = asset
                position['size_over_liq5'] = round((float(position['notional_size'])/1e3)/max(500, position['liq']), 2)
                position['size_over_liq10'] = round((float(position['notional_size'])/1e3)/max(500, liquidity['bid_10'], liquidity['ask_10']), 2)
                liq_size_check = position['size_over_liq5'] > 1
                liq_distance_check = position['distance'] < 0.15 if position['distance'] is not None else False
                strict_liq_distance_check = position['distance'] < 0.05 if position['distance'] is not None else False
                pos_size_check = position['size_over_liq10'] > 4
                pos_distance_check = position['distance'] < 0.2 if position['distance'] is not None else False
                hype_distance_check = position['distance'] < 0.2 if position['distance'] is not None else False
                combo = f'{acc}-{asset}'
                if acc in top_hype_holders_upnl and hype_distance_check:
                    top_hype_holders_upnl[acc] += position['unrealized_pnl']
                if liq_size_check and liq_distance_check:
                    liquidation_risks.append(position)
                    message = f'LIQ RISK: {position}'
                    if combo not in alerted_positions or strict_liq_distance_check:
                        alerted_positions.append(combo)
                        send_pushover_alert(message, priority=1)
                if pos_size_check:
                    large_positions.append(position)
                    message = f'POS RISK: {position}'
                    if combo not in alerted_positions:
                        alerted_positions.append(combo)
                        send_pushover_alert(message, priority=1)
            with open(f'./liquidation_risks/{asset}.json', "w") as f:
                json.dump(liquidation_risks, f, indent=4)
            with open(f'./large_positions/{asset}.json', "w") as f:
                json.dump(large_positions, f, indent=4)

    hype_holders_at_risk = [h for h in top_hype_holders_upnl if -1*top_hype_holders_upnl[h] > hype_liquidity['bid_5']]
    hype_at_risk = []
    for h in hype_holders_at_risk:
        for p in top_hype_positions:
            if p['user'] == h:
                p['upnl'] = top_hype_holders_upnl[h]
                hype_at_risk.append(p)
    with open(f'./key_stats/hype_at_risk.json', "w") as f:
        json.dump(hype_at_risk, f, indent=4)


if __name__ == '__main__':
    while True:
        try:
            summarise()
            time.sleep(60*30)
        except Exception as e:
            msg = f'Summarise call failed because {e}'
            send_pushover_alert(msg, priority=-1)
            time.sleep(60)

