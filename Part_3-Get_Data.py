from __future__ import print_function
import datetime
import psycopg2
import pandas as pd
import time 
import requests 
import json 
import config 


key= '608XXXXXXXXXXXXXXXXXXXX@AMER.OAUTHAP'

start_date = "01-04-2018"
end_date = "15-07-2022"

def obtain_list_db_tickers(conn):
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT id, ticker, market_cap, class FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1], d[2], d[3]) for d in data]
    
def unix_time(dt):
    date = int(time.mktime(datetime.datetime.strptime(dt, "%d-%m-%Y").timetuple())*1000)
    return date 


def load_data(symbol, symbol_id, conn, frequencyType):
    cur = conn.cursor()
    start_dt = unix_time(start_date)
    end_dt = unix_time(end_date)
    try: 
        periodType='year'
        period = 1
        #frequencyType='weekly'  #daily, weekly, monthly
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
    columns_table_order = ['asset_id', 'date_price', 'open_price', 'high_price', 
                           'low_price', 'close_price', 'volume']
    newDF = pd.DataFrame()
    newDF['date_price'] = data['date']
    newDF['open_price'] = data['open']
    newDF['high_price'] = data['high']
    newDF['low_price'] = data['low']
    newDF['close_price'] = data['close']
    newDF['volume'] = data['volume']
    newDF['asset_id'] = symbol_id
    newDF = newDF[columns_table_order]
    newDF = newDF.sort_values(by=['date_price'], ascending = True)
    return newDF


def main():
    db_user = config.db_user
    db_host = config.db_host
    db_name  = config.db_name
    db_password = config.db_password
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    stock_data = obtain_list_db_tickers(conn)
    frequencyType='daily'  #daily, weekly, monthly
     
    for stock in stock_data:
        symbol_id = stock[0]
        symbol = stock[1]
        print('Currently loading {}'.format(symbol))
        try:
            dta = load_data(symbol, symbol_id, conn, frequencyType)
            list_of_lists = dta.values.tolist()
            # # convert our list to a list of tuples       
            tuples_mkt_data = [tuple(x) for x in list_of_lists]
                
            # WRITE DATA TO DB
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
            
if __name__ == "__main__":
    main()

