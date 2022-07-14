from __future__ import print_function
import datetime
import psycopg2
import requests
import os
import pandas as pd 

def parse_nasdaq_list(): 
    screener = pd.read_csv('assets-100.csv', encoding = 'latin-1')
    symbols = []
    for i, j in screener.iterrows():
        symbols.append((j['Symbol'], j['Name'],  j['Market Cap'], j['Class']))
    return symbols 


def insert_symbols(symbols, db_host, db_user, db_password, db_name):
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    column_str = """
                 ticker, name, market_cap, class
                 """
    insert_str = ("%s, " * 4)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    with conn:
        cur = conn.cursor()
        cur.executemany(final_str, symbols)


def main():
    db_user = 'postgres'
    db_name = 'demos'
    db_host='127.0.0.1'
    db_password = 'rock'
    symbols = parse_nasdaq_list()
    insert_symbols(symbols, db_host, db_user, db_password, db_name)
    print("%s symbols were successfully added." % len(symbols))  

    
if __name__ == "__main__":
    main()   