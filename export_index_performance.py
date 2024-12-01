import sqlite3
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

# Connect to the SQLite database
conn = sqlite3.connect('market_data.db')

# Fetch `index_data` from the database
index_data_query = '''
    SELECT Date, Index_Value, Top_100_Stocks 
    FROM index_data
'''
index_data_df = pd.read_sql(index_data_query, conn)

# Fetch `index_composition` from the database
index_composition_query = '''
    SELECT Date, Ticker, Weight 
    FROM index_composition
'''
index_composition_df = pd.read_sql(index_composition_query, conn)

# Close the database connection
conn.close()

# --- Export to CSV ---
index_data_df.to_csv("index_data.csv", index=False)
print("Index data exported to 'index_data.csv'.")
index_composition_df.to_csv("index_composition.csv", index=False)
print("Index composition exported to 'index_composition.csv'.")

# --- Export to Excel ---
with pd.ExcelWriter("index_data_and_composition.xlsx") as writer:
    index_data_df.to_excel(writer, sheet_name="Index_Data", index=False)
    index_composition_df.to_excel(writer, sheet_name="Index_Composition", index=False)
print("Data exported to 'index_data_and_composition.xlsx'.")

# --- Export to PDF ---
def export_to_pdf(dataframe, filename, title, column_headers, truncate_column=None):
    """
    Export a DataFrame to a PDF file.

    Parameters:
    - dataframe: The DataFrame to export.
    - filename: The name of the output PDF file.
    - title: The title of the PDF document.
    - column_headers: A list of column headers for the table.
    - truncate_column: Name of the column to truncate (optional).
    """
    pdf_filename = filename
    c = canvas.Canvas(pdf_filename, pagesize=letter)
    width, height = letter

    # Title for the PDF
    c.setFont("Helvetica-Bold", 16)
    c.drawString(30, height - 40, title)

    # Draw table headers
    y_position = height - 80
    c.setFont("Helvetica-Bold", 10)
    for i, header in enumerate(column_headers):
        c.drawString(30 + i * 150, y_position, header)  # Correct the usage here by adding 'text'
    
    # Draw table rows
    y_position -= 20
    c.setFont("Helvetica", 8)
    for _, row in dataframe.iterrows():
        for i, value in enumerate(row):
            # Truncate the specified column if necessary
            if truncate_column and column_headers[i] == truncate_column:
                value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            text = str(value)
            c.drawString(30 + i * 150, y_position, text)  # Correct the usage here as well
        y_position -= 20
        if y_position < 100:  # Add new page if necessary
            c.showPage()
            y_position = height - 40

    # Save the PDF
    c.save()
    print(f"Data has been successfully exported to '{pdf_filename}'.")

# Export `index_data` to PDF
export_to_pdf(
    index_data_df,
    "index_data.pdf",
    "Index Data Report",
    ["Date", "Index Value", "Top 100 Stocks"],
    truncate_column="Top 100 Stocks"
)

# Export `index_composition` to PDF
export_to_pdf(
    index_composition_df,
    "index_composition.pdf",
    "Index Composition Report",
    ["Date", "Ticker", "Weight"]
)
