import sqlite3
import pandas as pd
import numpy as np

# Connect to the SQLite database
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# Fetch the market cap and stock price data for October 2024
start_date = '2024-10-01'
end_date = '2024-10-31'

# SQL query to get market cap data for October 2024
cursor.execute('''
    SELECT Date, Ticker, Market_Cap 
    FROM market_cap 
    WHERE Date BETWEEN ? AND ?
''', (start_date, end_date))

market_cap_data = cursor.fetchall()
market_cap_df = pd.DataFrame(market_cap_data, columns=['Date', 'Ticker', 'Market_Cap'])

# SQL query to get stock prices for October 2024
cursor.execute('''
    SELECT Date, Ticker, Close 
    FROM stock_prices 
    WHERE Date BETWEEN ? AND ?
''', (start_date, end_date))

stock_price_data = cursor.fetchall()
stock_price_df = pd.DataFrame(stock_price_data, columns=['Date', 'Ticker', 'Close'])

# Create a new table for the index data if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS index_data (
    Date TEXT,
    Index_Value REAL,
    Top_100_Stocks TEXT
)
''')

# Initialize a list to store the index data for each day
index_data = []

# Loop through each day in October 2024
for date in pd.date_range(start=start_date, end=end_date):

    # Get market cap data for the day
    daily_market_cap = market_cap_df[market_cap_df['Date'] == str(date.date())]
    
    # Get stock price data for the day
    daily_stock_prices = stock_price_df[stock_price_df['Date'] == str(date.date())]
    
    # Merge market cap and stock price data on Ticker
    merged_data = pd.merge(daily_market_cap, daily_stock_prices, on='Ticker')
    
    # Sort by Market Cap in descending order and pick the top 100 stocks
    top_100_stocks = merged_data.sort_values(by='Market_Cap', ascending=False).head(100)
    
    # Calculate the equal-weighted index for the day
    # In an equal-weighted index, each of the top 100 stocks contributes equally
    # The formula for index value on a given day is the sum of (Stock Price / Number of Stocks)
    
    equal_weight = 1 / 100  # Each stock contributes equally
    index_value = np.sum(top_100_stocks['Close'] * equal_weight)
    
    # Get the list of top 100 stock tickers
    top_100_tickers = ','.join(top_100_stocks['Ticker'].tolist())
    
    # Append the data to the index_data list
    index_data.append((str(date.date()), index_value, top_100_tickers))

# Insert the calculated index data into the index_data table
cursor.executemany('''
    INSERT INTO index_data (Date, Index_Value, Top_100_Stocks)
    VALUES (?, ?, ?)
''', index_data)
cursor.execute('''
CREATE TABLE IF NOT EXISTS index_composition (
    Date TEXT,
    Ticker TEXT,
    Weight REAL
)
''')

# Function to calculate and store the daily top 100 stocks
def calculate_index_composition(date):
    # Fetch market cap and stock prices for the selected date
    market_cap_df = pd.read_sql(f'''
        SELECT * FROM market_cap WHERE Date = "{date}"
    ''', conn)
    
    # Sort by market cap in descending order to get the top 100
    top_100_stocks = market_cap_df.nlargest(100, 'Market_Cap')
    
    # Calculate equal weight (1/100)
    top_100_stocks['Weight'] = 1 / 100
    
    # Insert the top 100 stocks into the index_composition table
    for _, row in top_100_stocks.iterrows():
        cursor.execute('''
            INSERT INTO index_composition (Date, Ticker, Weight)
            VALUES (?, ?, ?)
        ''', (row['Date'], row['Ticker'], row['Weight']))
    
    conn.commit()

# Generate index composition for each date in the market cap data
dates = pd.read_sql('SELECT DISTINCT Date FROM market_cap', conn)['Date'].tolist()
for date in dates:
    calculate_index_composition(date)

print("Index composition data has been successfully populated.")

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Index data has been successfully written to 'market_data.db'.")
