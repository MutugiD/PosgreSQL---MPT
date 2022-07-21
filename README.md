# PosgreSQL---MPT
This repo has a number of functionalities:  
1. Allow for scrapping for all NASDAQ listed tickers (stocks, REITs, mutual funds) data on daily basis.  
2. Once daily data is pumped into a PostGreSQL database, it will be used for creating a simple low volatility-return model.  
3. Use the best tickers per class to form a selection to be pumped into a diversified MPT portfolio.  

Process:   
Set up postgresDB schema, set up 2 tables - Symbols and the Daily Data tables.  
Insert symbols into the symbols table.  
Scrap all the data from TD Ameritrade API.  
Allow for a cron job scheduler to update the database every 12 hours.   
**Modelling**  
Starting off with market scanning for low-volatility tickers with higher than normal returns.   
