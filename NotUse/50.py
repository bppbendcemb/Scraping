import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import logging
import os  # For creating directories
from database import database
import pyodbc

# ------------------------------------------------------------------------------------------------
# Step 1: Scraping
url = 'http://bppnet/report/whiss.aspx'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        df = pd.DataFrame(rows, columns=headers)
        
        # Save to CSV (optional for reference)
        output_dir = 'CSV'
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, '50.csv'), index=False)
    else:
        print("No Table found on the page.")
else:
    print(f"Page Not Found: {response.status_code}")

# ------------------------------------------------------------------------------------------------
# Step 2: Process Data

 # Clean the 'จำนวนชิ้นงาน' column (remove commas and convert to int)
df['จำนวนชิ้นงาน'] = df['จำนวนชิ้นงาน'].str.replace(',', '').astype(int)

# Rename columns
rename_dict = {
    'ปี': 'yr',
    'เดือน': 'm',
    'จำนวนชิ้นงาน': 'pieces'
}
df.rename(columns=rename_dict, inplace=True)
# Ensure month values are treated as integers
df['m'] = df['m'].astype(int)

# Pivot the data for 'pieces' (จำนวนชิ้นงาน)
pieces_df = df.pivot_table(index='yr', columns='m', values='pieces', aggfunc='sum')

# Reindex to ensure all 12 months (1 to 12) are present, fill missing months with 0
months = list(range(1, 13))  # Ensures January (1) to December (12)
pieces_df = pieces_df.reindex(columns=months, fill_value=0)
# Sort the columns to ensure they are in the order of 1 to 12
pieces_df = pieces_df.reindex(sorted(pieces_df.columns), axis=1)

# Add metadata columns for output
kpi_id = 50  # Parameterized KPI ID
yr = df['yr'].iloc[0]
pieces_df.insert(0, 'uniqueid', str(yr) + str(kpi_id))
pieces_df.insert(1, 'yr', yr)
pieces_df.insert(2, 'kpi_id', kpi_id)

# Rename columns from numeric months to 'm01', 'm02', ..., 'm12'
pieces_df.columns = ['uniqueid', 'yr', 'kpi_id'] + [f'm{int(col):02}' for col in pieces_df.columns[3:]]

# Output file path
output_dir = r'CSV2'
output_path = os.path.join(output_dir, '50.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
pieces_df.to_csv(output_path, index=False)

# ------------------------------------------------------------------------------------------------
# Step 3: Save to Database
# Establish the database connection
conn = database()
# conn = database(server_choice='bbn6')

if conn:
    try:
        cursor = conn.cursor()
        print("Database connection established successfully.")
        
        # Your database operations here...
        # Iterate through the pieces_df DataFrame and insert or update data in SQL Server
        for index, row in pieces_df.iterrows():
            cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
            result = cursor.fetchone()[0]
    
            if result > 0:
                # Update existing record
                sql_update = """
                UPDATE KPI_dtl
                SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
                    m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
                WHERE unique_id = ?"""
                cursor.execute(sql_update, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                               row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                               row['m11'], row['m12'], row['yr'], row['uniqueid'])
                print(f"Updated row with unique_id = {row['uniqueid']}")
            else:
                # Insert new record
                sql_insert = """
                INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())"""
                cursor.execute(sql_insert, row['uniqueid'], row['yr'], row['kpi_id'],  
                               row['m01'], row['m02'], row['m03'], row['m04'], 
                               row['m05'], row['m06'], row['m07'], row['m08'], 
                               row['m09'], row['m10'], row['m11'], row['m12'])
                print(f"Inserted new row with unique_id = {row['uniqueid']}")
    
        # Commit changes to the database
        conn.commit()

    except Exception as e:
        print("An error occurred during database operations:", e)
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
else:
    print("Failed to establish a database connection.")