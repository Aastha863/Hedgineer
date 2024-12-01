import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# Polygon.io API details
API_BASE_URL_MARKET_CAP = "https://api.polygon.io/v3/reference/tickers/"
API_BASE_URL_STOCK_PRICES = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
API_KEY = "HQl8wq7i29q2Opa5anGVuTtLpmkWYtmG"  # Replace with your actual API key

# Configure logging
logging.basicConfig(filename="backfill_errors.log", level=logging.ERROR)

# Function to fetch market cap
def fetch_market_cap(ticker, date=None):
    # If date is provided, fetch data for that specific date
    if date:
        url = f"{API_BASE_URL_MARKET_CAP}{ticker}?date={date}"
    else:
        url = f"{API_BASE_URL_MARKET_CAP}{ticker}"  # Fetch the latest available market cap
    
    params = {"apiKey": API_KEY}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "OK":
                results = data.get("results", {})
                return {
                    "Ticker": results.get("ticker"),
                    "Name": results.get("name"),
                    "Market Cap": results.get("market_cap"),
                }
            else:
                logging.error(f"API Error for {ticker}: {data.get('status')}")
        else:
            logging.error(f"HTTP Error for {ticker}: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception for {ticker}: {e}")
    return None

# Function to fetch stock prices
def fetch_stock_prices(ticker, date):
    url = API_BASE_URL_STOCK_PRICES.format(ticker=ticker, start_date=date, end_date=date)
    params = {"adjusted": "true", "apiKey": API_KEY}
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if results:
                return {
                    "Ticker": ticker,
                    "Date": date,
                    "Open": results[0].get("o"),
                    "Close": results[0].get("c"),
                }
            else:
                logging.info(f"No stock price data for {ticker} on {date}")
        else:
            logging.error(f"HTTP Error for {ticker} on {date}: {response.status_code}")
    except Exception as e:
        logging.error(f"Exception for {ticker} on {date}: {e}")
    return None

# List of tickers to fetch (you can add more tickers here)
tickers = ["WGS", "NN", "OSW", "STGW", "SUPN", "HRMY", "IRON", "MBIN", "SJW", "SIMO", "CLOV", "MLKN", "SSRM", "AAOI", "SDGR", "MTTR", "KNSA", "XMTR", "SPNS", "WOOF", "DAWN", "NSSC", "OLPX", "OCSL"]
  # Add more tickers as needed

# Backfill for October 2024
start_date = datetime(2024, 10, 1)
end_date = datetime(2024, 10, 31)

# Generate list of trading days (skip weekends)
date_list = []
current_date = start_date
while current_date <= end_date:
    if current_date.weekday() < 5:  # Skip weekends (Monday=0, Sunday=6)
        date_list.append(current_date.strftime('%Y-%m-%d'))
    current_date += timedelta(days=1)

# Fetch and save data
market_cap_data = []
stock_price_data = []

for date in date_list:
    print(f"Fetching data for {date}...")
    for ticker in tickers:
        # Fetch market cap data
        market_cap = fetch_market_cap(ticker, date)
        if market_cap:
            market_cap["Date"] = date
            market_cap_data.append(market_cap)
        
        # Fetch stock price data
        stock_prices = fetch_stock_prices(ticker, date)
        if stock_prices:
            stock_price_data.append(stock_prices)
        
        # Optional delay to avoid hitting API rate limits
        time.sleep(10)

# Save market cap data to a CSV
if market_cap_data:
    market_cap_df = pd.DataFrame(market_cap_data)
    market_cap_df.to_csv("market_cap_oct_2024_6.csv", index=False)
    print("Market cap data saved to 'market_cap_oct_2024_6.csv'.")
else:
    print("No market cap data fetched.")

# Save stock price data to a CSV
if stock_price_data:
    stock_price_df = pd.DataFrame(stock_price_data)
    stock_price_df.to_csv("stock_prices_oct_2024_6.csv", index=False)
    print("Stock price data saved to 'stock_prices_oct_2024_6.csv'.")
else:
    print("No stock price data fetched.")
