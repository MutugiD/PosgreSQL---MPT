import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import config 
import os



def create_db(db_credentials):
    db_host, db_user, db_password, db_name = db_credentials
    if check_db_exists(db_credentials):
        pass
    else:
        print('Creating new database.')
        conn = psycopg2.connect(host=db_host, database='postgres', user=db_user, password=db_password, port= '5432')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE %s  ;" % db_name)
        cur.close()


def check_db_exists(db_credentials):
    db_host, db_user, db_password, db_name = db_credentials
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.close()
        print('Database exists.')
        return True
    except:
        print("Database does not exist.")
        return False
    
      
def create_mkt_tables(db_credentials):
    db_host, db_user, db_password, db_name = db_credentials
    conn = None
    if check_db_exists(db_credentials):
        commands = (
                    """
                    CREATE TABLE symbol (
                        id SERIAL PRIMARY KEY,
                        ticker TEXT NOT NULL,
                        name TEXT NOT NULL,
                        market_cap NUMERIC, 
                        class TEXT NOT NULL
                        )
                    """,
                    """
                    CREATE TABLE daily_data (
                        id SERIAL PRIMARY KEY,
                        asset_id INTEGER NOT NULL,
                        date_price DATE,
                        open_price NUMERIC,
                        high_price NUMERIC,
                        low_price NUMERIC,
                        close_price NUMERIC,
                        volume BIGINT,
                        FOREIGN KEY (asset_id) REFERENCES symbol(id)
                        )                    
                    """)
        try:
            for command in commands:
                print('Building tables.')
                conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
                cur = conn.cursor()
                cur.execute(command)
                conn.commit()
                cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            cur.close()
        finally:
            if conn:
                conn.close()
    else:
        pass


def main():
    db_user = config.db_user
    db_host = config.db_host
    db_name  = config.db_name
    db_password = config.db_password
    create_db([db_host, db_user, db_password, db_name])
    create_mkt_tables([db_host, db_user, db_password, db_name])
   
if __name__ == "__main__":
    main()
