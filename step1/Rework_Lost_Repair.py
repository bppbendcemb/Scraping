import os
import requests
from bs4 import BeautifulSoup
import chardet
import pandas as pd
import numpy as np
from datetime import datetime

# Function to extract table data and combine them
def get_table_data(soup, table_ids):
    table_data = []
    for table_id in table_ids:
        table = soup.find(id=table_id)
        if table:
            headers = [header.text.strip() for header in table.find_all('th')]
            rows = [[td.text.strip() for td in row.find_all('td')] for row in table.find_all('tr')[1:]]
            table_df = pd.DataFrame(rows, columns=headers)
            table_data.append(table_df)
        else:
            print(f"ไม่พบตารางที่มี id '{table_id}'")
    return pd.concat(table_data, ignore_index=True) if table_data else pd.DataFrame()

# URL of the website to scrape
url = 'http://bppnet/report/kpi/kpipdrw.aspx'

# Make HTTP request to fetch the content from the website
response = requests.get(url)

# Detect encoding of the content
result = chardet.detect(response.content)
encoding = result['encoding']
response.encoding = encoding

# Parse HTML content using BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Create output directory if it doesn't exist
folder_Output = 'Output'
if not os.path.exists(folder_Output):
    os.makedirs(folder_Output)

# List of table IDs to scrape
table_ids = [
    'MainContent_GridView1', 
    'MainContent_GridView2', 
    'MainContent_GridView5'
]

# Collect data from each table
df = get_table_data(soup, table_ids)

# Get the current year
current_year = datetime.now().year

# Ensure 'ปี' column exists and is of type int
if 'ปี' not in df.columns:
    df['ปี'] = np.nan  # Initialize with NaN

# Replace empty strings and non-numeric values with NaN
df['ปี'] = df['ปี'].replace('', np.nan)

# Convert 'ปี' column to numeric
df['ปี'] = pd.to_numeric(df['ปี'], errors='coerce')

# Fill NaN values with the current year and convert to integer
df['ปี'] = df['ปี'].fillna(current_year).astype(int)

# Create uniqueid by combining 'ปี' and 'รายการ'
df['uniqueid'] = df.apply(lambda row: f"{row['ปี']}_{row['รายการ']}", axis=1)

# Save the updated DataFrame to a new CSV file
updated_file_name = 'Rework_Lost_Repair.csv'
updated_file_path = os.path.join(folder_Output, updated_file_name)
df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')

print(f"Year column updated to the current year and data saved to '{updated_file_path}'.")

# Read the CSV file into a DataFrame
df = pd.read_csv(updated_file_path)

# Rename columns
df.rename(columns={
    'ปี': 'yr',
    'รายการ': 'desc',
    'ม.ค.': 'm01',
    'ก.พ.': 'm02',
    'มี.ค.': 'm03',
    'เม.ย.': 'm04',
    'พ.ค.': 'm05',
    'มิ.ย.': 'm06',
    'ก.ค.': 'm07',
    'ส.ค.': 'm08',
    'ก.ย.': 'm09',
    'ต.ค.': 'm10',
    'พ.ย.': 'm11',
    'ธ.ค.': 'm12'
}, inplace=True)

# Save the updated DataFrame back to a CSV file
df.to_csv(updated_file_path, index=False,encoding='utf-8-sig')

print(f"Columns renamed and data saved to '{updated_file_path}'.")
