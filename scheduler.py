import schedule
import scrape_binance as sb
import time

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT', 'LINKUSDT']

for symbol in SYMBOLS:
    schedule.every(1).minutes.do(sb.scrape_scheduler, symbol=symbol, kline_size='1m')


while True:
    schedule.run_pending()
