import requests
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import csv
import time
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Define the base URL for the API
base_url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"

# Set the date for the query
date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  # Corrected part
api_key = "HQl8wq7i29q2Opa5anGVuTtLpmkWYtmG"  # Replace with your own API key

# Construct the full URL with the date
url = f"{base_url}{date}"

# Set the parameters for the API call
params = {
    "adjusted": "true",  # Set to "false" if you want unadjusted data
    "include_otc": "false",  # Set to "true" to include OTC securities (optional)
    "apiKey": api_key  # Pass the API key
}

# Make the GET request to the API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()  # Parse the JSON response

    # Extract the 'results' part from the response
    results = data.get("results", [])

    # Check if we have any results
    if results:
        # Convert the results into a pandas DataFrame
        df = pd.DataFrame(results)

        # Convert the Unix timestamp (t) into a human-readable date
        df['Date'] = pd.to_datetime(df['t'], unit='ms')

        # Drop the original timestamp column
        df.drop(columns=['t'], inplace=True)

        # Save the DataFrame to a CSV file
        df.to_csv('stock_data.csv', index=False)

        # Print confirmation
        print("Data has been saved to 'stock_data.csv'.")
    else:
        print("No data found for the given date.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# Extract the company symbols
list_companies = df['T'].to_list()

json_file_path = "service_account.json"

# Define the scope
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Authenticate using the JSON file
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_path, scope)
gc = gspread.authorize(credentials)

# Open the Google Sheet by URL
sheet_url = "https://docs.google.com/spreadsheets/d/1mOK1BuZyD3n24ilCllSFMkLo5crJbUjXiCrP53_OTno/edit?gid=0"
spreadsheet = gc.open_by_url(sheet_url)

# Select the first worksheet
worksheet = spreadsheet.get_worksheet(0)

# Prepare data with formulas for the market cap
data_to_write = [[symbol, f'=GOOGLEFINANCE("NASDAQ:{symbol}", "marketcap")'] for symbol in list_companies]

# Add column headers
data_to_write.insert(0, ["Symbol", "Market Cap"])

# Update the worksheet starting from cell A1
worksheet.update("A1", data_to_write, value_input_option='USER_ENTERED')  # Ensures formulas are treated as formulas

print("Data with formulas written successfully!")

# Open the source spreadsheet to retrieve the calculated data
source_sheet_url = "https://docs.google.com/spreadsheets/d/1mOK1BuZyD3n24ilCllSFMkLo5crJbUjXiCrP53_OTno/edit?gid=0#gid=0"
destination_sheet_url = "https://docs.google.com/spreadsheets/d/1mOK1BuZyD3n24ilCllSFMkLo5crJbUjXiCrP53_OTno/edit?gid=817291030#gid=817291030"

# Open the source spreadsheet
source_spreadsheet = gc.open_by_url(source_sheet_url)

# Select the first worksheet in the source spreadsheet
source_worksheet = source_spreadsheet.get_worksheet(0)

# Retrieve all data (evaluated values) from the source worksheet
source_data = source_worksheet.get_all_values()

# Convert the data to a DataFrame
df = pd.DataFrame(source_data)

# Assign the first row as headers and remove the header row from the data
df.columns = df.iloc[0]
df = df[1:]

# Replace '#NA' values with NaN and ensure there are no unwanted string representations
df.replace({'#NA': np.nan, '#N/A': np.nan, 'N/A': np.nan}, inplace=True)

# Remove rows with any NaN (missing) values
df_cleaned = df.dropna()

df['Market Cap'] = df['Market Cap'].astype(float)

# Sort by market_Cap in descending order
df_sorted = df.sort_values(by='Market Cap', ascending=False)
df_sorted.to_csv('final_data.csv', index=False)
# Get the top 1000 symbols
tickers = df_sorted.head(1000)['Symbol'].tolist()


API_BASE_URL = "https://api.polygon.io/v3/reference/tickers/"
API_KEY = "HQl8wq7i29q2Opa5anGVuTtLpmkWYtmG"

# List of tickers

# Fetch data for each ticker
logging.basicConfig(filename="market_cap_errors.log", level=logging.ERROR)



# Function to fetch market cap using Polygon API
def fetch_market_cap(ticker):
    url = f"{API_BASE_URL}{ticker}"
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

# Fetch market cap for all tickers
data_list = []
for ticker in tickers:
    print(f"Fetching data for {ticker}...")
    result = fetch_market_cap(ticker)
    if result:
        data_list.append(result)
    # time.sleep(1)  # Delay to avoid hitting rate limits

# Convert results to DataFrame and save
if data_list:
    df = pd.DataFrame(data_list)
    df.to_csv("market_cap_data.csv", index=False)
    print("Market cap data saved to 'market_cap_data.csv'.")
else:
    print("No data fetched.")
