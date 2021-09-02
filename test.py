import scrape_binance as sb
import datetime as dt
import database as db
import pandas as pd

#sb.scrape_historical('ETHUSDT', '1m', dt.datetime(year=2017, month=8, day=1))
#sb.scrape_historical('BNBUSDT', '1m', dt.datetime(year=2017, month=8, day=1))
#sb.scrape_historical('LINKUSDT', '1m', dt.datetime(year=2017, month=8, day=1))
#sb.scrape_historical('LTCUSDT', '1m', dt.datetime(year=2017, month=8, day=1))
sb.scrape_scheduler.run(symbol='BTCUSDT', kline_size='1m')


#with db.get_connection() as con:
#    df = pd.read_sql('select * from common.coins', con)


