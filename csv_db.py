import pandas as pd
import sqlite3

# Load the CSV files
market_cap_df = pd.read_csv('market_cap_oct_2024.csv')
stock_prices_df = pd.read_csv('stock_prices_oct_2024.csv')

# Connect to SQLite database (it will create the file if it doesn't exist)
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# Create the 'market_cap' table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS market_cap (
    Date TEXT,
    Ticker TEXT,
    Market_Cap INTEGER
)
''')

# Create the 'stock_prices' table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS stock_prices (
    Date TEXT,
    Ticker TEXT,
    Close REAL
)
''')

# Insert data into the 'market_cap' table
for _, row in market_cap_df.iterrows():
    cursor.execute('''
    INSERT INTO market_cap (Date, Ticker, Market_Cap)
    VALUES (?, ?, ?)
    ''', (row['Date'], row['Ticker'], row['Market Cap']))

# Insert data into the 'stock_prices' table
for _, row in stock_prices_df.iterrows():
    cursor.execute('''
    INSERT INTO stock_prices (Date, Ticker, Close)
    VALUES (?, ?, ?)
    ''', (row['Date'], row['Ticker'], row['Close']))

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Data has been successfully written to 'market_data.db'")
