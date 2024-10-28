import os
import requests
from bs4 import BeautifulSoup
import chardet
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import pyodbc
from sqlalchemy import create_engine
import csv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
            logging.warning(f"Table with id '{table_id}' not found.")
    return pd.concat(table_data, ignore_index=True) if table_data else pd.DataFrame()

# URL of the website to scrape
url = 'http://bppnet/report/kpi/kpipdrw.aspx'

try:
    # Make HTTP request to fetch the content from the website
    response = requests.get(url)
    response.raise_for_status()
    
    # Detect encoding of the content
    result = chardet.detect(response.content)
    encoding = result['encoding']
    response.encoding = encoding

    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

except requests.RequestException as e:
    logging.error(f"Error fetching the URL: {e}")
    raise

# Create output directory if it doesn't exist
folder_Output = 'step1\Output'
if not os.path.exists(folder_Output):
    os.makedirs(folder_Output)

# List of table IDs to scrape
table_ids = ['ctl00_MainContent_GridView1', 'ctl00_MainContent_GridView2', 'ctl00_MainContent_GridView5']

# ctl00_MainContent_GridView1

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

# Custom mapping of 'รายการ' to 'id'
item_id_mapping = {
    'การ Set Up เครื่อง/แม่พิมพ์ (MH)': 1,
    'จากเครื่องจักรเสีย (MH)': 2,
    'จากแม่พิมพ์เสีย (MH)': 3,
    'จากการรอวัสดุ/อุปกรณ์ (MH)': 4,
    'จากการไปผลิต/ทำงานอื่น (MH)': 5,
    'อื่น ๆ (MH) (ที่นอกเหนือหัวข้อข้างต้น)': 6,
    'MH ผลิตจริง (เฉพาะทีปิด)': 7,
    '% ซ่อมสี:': 18,
    'เวลาสูญเสียจากการผลิต ต้องไม่เกิน': 17,
    'สาเหตุจากพนักงาน(ชิ้น)': 20,
    'สาเหตุจากวัตถุดิบ(ชิ้น)': 21,
    'วัตถุดิบเฉพาะ Dis 442/Com054 (ชิ้น)': 22,
    'สาเหตุจากเครื่องจักร(ชิ้น)': 23,
    'สาเหตุจากวิธีการ/ควบคุม(ชิ้น)': 24,
    'สาเหตุจากแม่พิมพ์ (ชิ้น)': 25,
    'แม่พิมพ์เฉพาะ (Dis 177,258/Com054) (ชิ้น)': 26,
    'สาเหตุจากงานจ้างผลิตภายนอก (ชิ้น)': 27,
    'สาเหตุจากการออกแบบ (ชิ้น)': 28,
    'สาเหตุจากการซ่อมสี': 29,
    'สาเหตุจากคำสั่งพิเศษ': 30,
    'สาเหตุจากข้อร้องเรียนลูกค้า': 31,
    'ยอดผลิตรวม(ชิ้น)': 99,
    'จำนวนพ่นสีทั้งหมด': 100,
    'จำนวนงานซ่อมสี': 101
}

# Ensure 'รายการ' column exists before proceeding
if 'รายการ' in df.columns:
    # Map 'รายการ' to 'id' using the provided mapping
    df['id'] = df['รายการ'].map(item_id_mapping)

    # Convert 'id' column to int (in case there are NaN values, they will be replaced with -1 or some default value)
    df['id'] = df['id'].fillna(-1).astype(int)

    # logging.info("Mapped 'รายการ' to 'id'.")
else:
    logging.warning("Column 'รายการ' not found in the data.")

# Save the updated DataFrame to a new CSV file
updated_file_name = 'ReworkLost.csv'
updated_file_path = os.path.join(folder_Output, updated_file_name)
df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')


# ----------------------------------------------------------------------------------------------------------
# Read the CSV file into a DataFrame
df = pd.read_csv(updated_file_path)

# Rename columns
rename_dict = {
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
}

# Check if columns to rename exist
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)
    df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    # logging.info(f"Columns renamed and data saved to '{updated_file_path}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# ตรวจสอบว่ามี ID 17 อยู่ใน DataFrame หรือไม่
special_id = 17
if special_id not in df['id'].values:
    # เพิ่มแถวใหม่สำหรับ ID 17
    special_row = pd.DataFrame([{
        'yr': 2024,
        'desc': 'เวลาสูญเสียจากการผลิต ต้องไม่เกิน',
        'm01': 0, 'm02': 0, 'm03': 0, 'm04': 0, 'm05': 0, 'm06': 0,
        'm07': 0, 'm08': 0, 'm09': 0, 'm10': 0, 'm11': 0, 'm12': 0,
        'id': special_id
    }])
    df = pd.concat([df, special_row], ignore_index=True)

    # Sort DataFrame by 'id'
    df = df.sort_values(by='id')

    df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    # logging.info("เพิ่มแถวใหม่สำหรับ ID 17.")
else:
    logging.info("ID 17 มีอยู่ใน DataFrame แล้ว")

# ----------------------------------------------------------------------------------------------------------
# Load the CSV file
df = pd.read_csv(updated_file_path)

# Convert the 'm01' to 'm12' columns to numeric, coercing errors to NaN
sum_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
df[sum_columns] = df[sum_columns].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')

# Sum the values in 'm01' to 'm12' for rows with id 2 to 6
sum_values = df[df['id'].isin([2, 3, 4, 5, 6])][sum_columns].sum()

# Update only the row with id = 17
df.loc[df['id'] == 17, sum_columns] = sum_values.values

# Save the updated DataFrame back to the CSV file
df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')

# print(f"Updated 'id = 17' with sums of columns {sum_columns}.")

# ----------------------------------------------------------------------------------------------------------
# Load the CSV file
df = pd.read_csv(updated_file_path)

# List of ids to process
ids = [17, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]

# Define the month columns
months = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

# Identify the necessary rows (with checks)
if not df[df['id'] == 7].empty and not df[df['id'] == 99].empty:
    row_7 = df[df['id'] == 7].iloc[0]
    row_99 = df[df['id'] == 99].iloc[0]
    
    # Ensure no division by zero
    row_7_values = row_7[months].astype(float).replace(0, float('nan'))
    row_99_values = row_99[months].astype(float).replace(0, float('nan'))

    for id_val in ids:
        if not df[df['id'] == id_val].empty:
            row = df[df['id'] == id_val].iloc[0]
            
            # Choose different calculation for row_17 and others
            if id_val == 17:
                result = (row[months].astype(float) / row_7_values) * 100
            else:
                result = (row[months].astype(float) / row_99_values) * 1000000
            
            # Update the values in the DataFrame
            df.loc[df['id'] == id_val, months] = result.values
            
            # print(f"Row with id={id_val} updated successfully!")
        else:
            print(f"Row with id={id_val} does not exist in the DataFrame.")
    
    # Create the new column 'activityid' by combining 'yr' and 'id'
    df['activityid'] = df['yr'].astype(str) + df['id'].astype(str)

    # Move 'activityid' to the first position
    cols = ['activityid'] + [col for col in df.columns if col != 'activityid']
    df = df[cols]

    # Save the updated DataFrame back into the CSV
    df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    print("All updates completed successfully!")
else:
    print("One or both of the rows with id=7 or id=99 are missing.")

# __________________

def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except ValueError:
        return None

# Configure logging
# logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

input_file = r'F:\_BPP\Project\Scraping\step1\Output\ReworkLost.csv'

with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ReworkLost' AND xtype='U')
CREATE TABLE ReworkLost (
    activityid INT PRIMARY KEY,
    yr INT,
    id INT,
    [desc] NVARCHAR(MAX),
    m01 FLOAT,
    m02 FLOAT,
    m03 FLOAT,
    m04 FLOAT,
    m05 FLOAT,
    m06 FLOAT,
    m07 FLOAT,
    m08 FLOAT,
    m09 FLOAT,
    m10 FLOAT,
    m11 FLOAT,
    m12 FLOAT,
    update_date DATETIME,
    create_date DATETIME
)
"""

check_sql = "SELECT COUNT(*) FROM ReworkLost WHERE activityid = ?"
column_names = ['activityid', 'yr', 'id', '[desc]', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

insert_sql = f"""
INSERT INTO ReworkLost ({', '.join(column_names)}, create_date)
VALUES ({', '.join(['?'] * len(column_names))}, Getdate())
"""

update_sql = """
UPDATE ReworkLost
SET yr = ?, id = ?, [desc] = ?, m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, m09 = ?, m10 = ?, m11 = ?, m12 = ?, update_date = GETDATE()
WHERE activityid = ?
"""

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            for row in data[1:]:
                if len(row) < 17:
                    # logging.warning(f"Skipping row due to insufficient columns: {row}")
                    row.extend([None] * (17 - len(row)))  # Fill missing columns with None

                uniqueid = row[0]
                cursor.execute(check_sql, uniqueid)
                exists = cursor.fetchone()[0]
# activityid,yr,desc,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12,id
                if exists:
                    cursor.execute(update_sql, (
                        row[1],
                        row[15], 
                        row[2],         
                        try_float(row[3]),  
                        try_float(row[4]), 
                        try_float(row[5]), 
                        try_float(row[6]),
                        try_float(row[7]), 
                        try_float(row[8]), 
                        try_float(row[9]), 
                        try_float(row[10]), 
                        try_float(row[11]),
                        try_float(row[12]), 
                        try_float(row[13]), 
                        try_float(row[14]),                       
                        uniqueid
                    ))
                else:
                    cursor.execute(insert_sql, (
                        uniqueid, 
                        row[1],
                        row[15], 
                        row[2],
                        try_float(row[3]),                       
                        try_float(row[4]), 
                        try_float(row[5]), 
                        try_float(row[6]),
                        try_float(row[7]), 
                        try_float(row[8]), 
                        try_float(row[9]), 
                        try_float(row[10]), 
                        try_float(row[11]),
                        try_float(row[12]), 
                        try_float(row[13]), 
                        try_float(row[14]),                                  
                    ))

            conn.commit()
    print("เพิ่มหรืออัปเดตข้อมูลเรียบร้อยแล้ว")
except pyodbc.Error as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")
