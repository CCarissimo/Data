import os
import scrape_binance as sb
from datetime import timedelta
from prefect.executors import DaskExecutor
from prefect import Flow
from prefect.schedules import IntervalSchedule


scheduler = IntervalSchedule(interval=timedelta(seconds=60))
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT', 'LINKUSDT']


def prefect_flow():
    with Flow(name='binance', schedule=scheduler) as flow:
        for symbol in SYMBOLS:
            sb.scrape_scheduler(symbol=symbol, kline_size='1m')

    return flow

    

if __name__ == '__main__':
    flow = prefect_flow()
    
    flow.run(executor=DaskExecutor())



