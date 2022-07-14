import psycopg2
import os
import pandas as pd 
import time 
from datetime import date, datetime, timedelta
import datetime
import requests 
import json 
import schedule
import time
import warnings
warnings.simplefilter("ignore")


key= 'XXXXXXXXXXX@AMER.OAUTHAP'

def unix_time(dt):
    date = int(time.mktime(datetime.datetime.strptime(dt, "%d-%m-%Y").timetuple())*1000)
    return date 

def obtain_list_db_tickers(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT id, ticker, class FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1], d[2]) for d in data]

def load_data(symbol, symbol_id, conn, frequencyType):
    cur = conn.cursor()
    query_date = "SELECT max(date_price) FROM daily_data"
    dates = pd.read_sql(query_date, conn)
    start_dt = unix_time(dates.iloc[-1, 0].strftime("%d-%m-%Y"))
    end_dt = unix_time(date.today().strftime("%d-%m-%Y"))
    try: 
        periodType='year'
        period = 1
        frequency=1
        endpoint = 'https://api.tdameritrade.com/v1/marketdata/{stock_ticker}/pricehistory?periodType={periodType}&period={period}\
                            &frequencyType={frequencyType}&frequency={frequency}&startDate={startDate}&endDate={endDate}'
        full_url = endpoint.format(stock_ticker=symbol, periodType=periodType,period=period,frequencyType=frequencyType,
                                   frequency=frequency, startDate=start_dt, endDate= end_dt)
        page = requests.get(url=full_url, params={'apikey' :key})
        content = json.loads(page.content)
        data= pd.DataFrame(content['candles'])
        data['date'] = pd.to_datetime(data['datetime'], unit='ms')
        data.drop(columns = ['datetime'], inplace = True)
        cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        data = data[cols]
    except: 
        raise Exception('Failed to load {}'.format(symbol))
       
    # data['date'] = data.index
    columns_table_order = ['stock_id', 'date_price', 'open_price', 'high_price', 
                           'low_price', 'close_price', 'volume']
    newDF = pd.DataFrame()
    newDF['date_price'] = data['date']
    newDF['open_price'] = data['open']
    newDF['high_price'] = data['high']
    newDF['low_price'] = data['low']
    newDF['close_price'] = data['close']
    newDF['volume'] = data['volume']
    newDF['stock_id'] = symbol_id
    newDF = newDF[columns_table_order]
    newDF = newDF.sort_values(by=['date_price'], ascending = True)
    return newDF


def main():
    db_user = 'postgres'
    db_name = 'assets'
    db_host='127.0.0.1'
    db_password = 'XXX'
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    stock_data = obtain_list_db_tickers(conn)
    frequencyType='daily'  
    for stock in stock_data:
        symbol_id = stock[0]
        symbol = stock[1]
        print('Currently loading {}'.format(symbol))
        try:
            dta = load_data(symbol, symbol_id, conn, frequencyType)
            list_of_lists = dta.values.tolist()
            tuples_mkt_data = [tuple(x) for x in list_of_lists]
                
            # WRITE DATA TO DB
           
            # WHERE date_price NOT IN (SELECT date_price FROM daily_data)
            # WHERE NOT EXISTS (select date_price from daily_data)
            insert_query =  """
                            INSERT INTO daily_data (asset_id, date_price, open_price, high_price, 
                                low_price, close_price, volume) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
            cur = conn.cursor()
            cur.executemany(insert_query, tuples_mkt_data)
            conn.commit()
        except:
            continue

            
def job():
    job = main()
    print("Updating after 12 hours..")
    
schedule.every(15).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(15)
