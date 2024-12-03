import pandas as pd
import sqlite3
import plotly.express as px
import streamlit as st

# Function to load data from the SQLite database
def load_data():
    # Connect to the SQLite database
    conn = sqlite3.connect('market_data.db')
    
    # Fetch index performance data for October 2024 (from 2024-10-01 to 2024-10-31)
    query_index_data = '''
    SELECT Date, Index_Value FROM index_data
    WHERE Date BETWEEN "2024-10-01" AND "2024-10-31"
    '''
    index_data = pd.read_sql(query_index_data, conn)
    
    # Fetch index composition data for October 2024
    query_composition_data = '''
    SELECT Date, Ticker, Weight FROM index_composition
    WHERE Date BETWEEN "2024-10-01" AND "2024-10-31"
    '''
    composition_data = pd.read_sql(query_composition_data, conn)
    
    # Close the database connection
    conn.close()

    return index_data, composition_data

# Load data from the SQLite database
index_data, composition_data = load_data()

# Streamlit page configuration
st.set_page_config(page_title="Index Performance and Composition", layout="wide")

# Title for the dashboard
st.title("Index Performance and Composition Dashboard (October 2024)")

# Section 1: Index Performance Over Time (Line Chart)
st.header("Index Performance (October 2024)")

# Plot the index performance using Plotly
fig = px.line(index_data, x='Date', y='Index_Value', title='Index Performance Over Time (October 2024)', markers=True)
fig.update_layout(xaxis_title="Date", yaxis_title="Index Value")
st.plotly_chart(fig)

# Section 2: Index Composition for Selected Date (Table or Bar Chart)
st.header("Index Composition on Selected Day")

# Dropdown to select a specific date
selected_date = st.selectbox("Select a Date", index_data['Date'].unique())

# Filter the composition data for the selected date
selected_composition = composition_data[composition_data['Date'] == selected_date]

# Display index composition as a table
st.subheader(f"Index Composition on {selected_date}")
st.dataframe(selected_composition)

# Add download button for CSV export, dropping the index column
csv = selected_composition.to_csv(index=False)
st.download_button(
    label="Download Composition as CSV",
    data=csv,
    file_name=f"index_composition_{selected_date}.csv",
    mime="text/csv"
)

# Alternatively, display composition as a bar chart
st.subheader("Index Composition - Bar Chart")
fig2 = px.bar(selected_composition, x='Ticker', y='Weight', title=f'Index Composition on {selected_date}')
fig2.update_layout(xaxis_title="Ticker", yaxis_title="Weight", showlegend=False)
st.plotly_chart(fig2)

# Section 3: Highlight Composition Changes (Bar Chart)
st.header("Composition Changes in October 2024")

# Track composition changes by comparing consecutive days' top 100 stocks
composition_changes = []
dates = sorted(composition_data['Date'].unique())

# Iterate over consecutive days and track changes in the index composition
for i in range(1, len(dates)):
    prev_date = dates[i - 1]
    curr_date = dates[i]
    
    # Get the top 100 tickers for the previous and current date
    prev_composition = composition_data[composition_data['Date'] == prev_date]
    curr_composition = composition_data[composition_data['Date'] == curr_date]
    
    prev_tickers = set(prev_composition['Ticker'])
    curr_tickers = set(curr_composition['Ticker'])
    
    # Identify tickers added and removed
    added = curr_tickers - prev_tickers
    removed = prev_tickers - curr_tickers
    
    if added or removed:
        composition_changes.append({
            'Date': curr_date,
            'Added': len(added),
            'Removed': len(removed),
            'Total Changes': len(added) + len(removed)
        })

# Convert the composition changes into a DataFrame
composition_changes_df = pd.DataFrame(composition_changes)

# Plot the composition changes
fig3 = px.bar(composition_changes_df, x='Date', y='Total Changes', title="Composition Changes in October 2024")
fig3.update_layout(xaxis_title="Date", yaxis_title="Number of Changes", showlegend=False)
st.plotly_chart(fig3)

# Section 4: Summary Metrics (Cumulative Returns and Daily % Changes)
st.header("Summary Metrics")

# Clean and preprocess index data
index_data['Date'] = pd.to_datetime(index_data['Date'])
index_data.sort_values(by='Date', inplace=True)

# Convert 'Index_Value' to numeric, forcing non-numeric values to NaN
index_data['Index_Value'] = pd.to_numeric(index_data['Index_Value'], errors='coerce')

# Drop rows with NaN values in 'Index_Value' after conversion
index_data = index_data.dropna(subset=['Index_Value'])

# Calculate daily percentage changes
index_data['Daily Change (%)'] = index_data['Index_Value'].pct_change() * 100

# Drop any rows where 'Daily Change (%)' is NaN
index_data = index_data.dropna(subset=['Daily Change (%)'])

st.subheader("Daily Percentage Changes")
if not index_data.empty:
    st.write(index_data[['Date', 'Daily Change (%)']].set_index('Date'))
else:
    st.write("No data available for daily percentage changes.")

# Show number of composition changes
st.subheader("Number of Composition Changes")
st.write(f"Total Composition Changes in October 2024: {composition_changes_df.shape[0]}")
