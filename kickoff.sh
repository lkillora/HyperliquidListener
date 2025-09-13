nohup python3 liquidity.py > liquidity.log 2>&1 &
nohup python3 positions.py > positions.log 2>&1 &
nohup python3 prices.py > prices.log 2>&1 &
nohup python3 summary_stats.py > summary_stats.log 2>&1 &
nohup python3 hydromancer_ws_filters.py > hydromancer_ws_filters.log 2>&1 &