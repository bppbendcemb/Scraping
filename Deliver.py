import os
import requests
from bs4 import BeautifulSoup
import csv
import logging
import pyodbc
import pandas as pd
import numpy as np

url = 'http://bppnet/report/whiss.aspx'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'Deliver1.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("ข้อมูลถูกบันทึกลงในไฟล์ Deliver1.csv")
    else:
        print("ไม่พบตารางที่ระบุ")
else:
    print(f"ไม่สามารถเข้าถึงหน้าเว็บได้: {response.status_code}")

# ***************************
# เปลี่ยนชื่อหัวตารางเป็นภาษาอังกฤษ
# input_file = r'F:\_BPP\Project\Scraping\Output\KPI_Send.csv'
updated_file_name = 'Deliver1.csv'
updated_file_path = os.path.join(folder_Output, updated_file_name)
df = pd.read_csv(updated_file_path)

#ปี,เดือน,จำนวนรายการ,จำนวนชิ้นงาน
df['จำนวนชิ้นงาน'] = df['จำนวนชิ้นงาน'].str.replace(',', '').astype(int)

# Rename columns
rename_dict = {
    'ปี': 'yr',
    'เดือน': 'm',
    'จำนวนรายการ': 'items',
    'จำนวนชิ้นงาน': 'pieces'
}


# Check if columns to rename exist
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)
    df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    # logging.info(f"Columns renamed and data saved to '{updated_file_path}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")

    
# ***************************

# Load the data from the CSV file
df = pd.read_csv(updated_file_path)

# Extract the year from the data (assuming it's the same for all rows)
yr = df['yr'].iloc[0]

# Pivot the data for items and pieces
items_df = df.pivot(index='yr', columns='m', values='items')
pieces_df = df.pivot(index='yr', columns='m', values='pieces')


# Reindex to ensure months 1 to 12 are present
months = list(range(1, 13))
items_df = items_df.reindex(columns=months, fill_value=np.nan)
pieces_df = pieces_df.reindex(columns=months, fill_value=np.nan)

# Convert columns to object type to handle mixed data (numbers and '#N/A')
items_df = items_df.astype(object)
pieces_df = pieces_df.astype(object)

# Replace NaN with #N/A
# items_df.fillna('#N/A', inplace=True)
# pieces_df.fillna('#N/A', inplace=True)

items_df.fillna('None', inplace=True)
pieces_df.fillna('None', inplace=True)

# Add additional columns for activityid and genre
items_df.insert(0, 'uniqueid', str(yr) + str(501))
items_df.insert(1, 'yr', yr)
items_df.insert(2, 'kpi_id', 501)
items_df.insert(3, 'genre', 'จำนวนรายการ')

pieces_df.insert(0, 'uniqueid',  str(yr) + str(50))
pieces_df.insert(1, 'yr', yr)
pieces_df.insert(2, 'kpi_id', 50)
pieces_df.insert(3, 'genre', 'จำนวนชิ้นงาน')

# Combine both DataFrames
final_df = pd.concat([items_df, pieces_df])

# Save to CSV
output_file = 'step1\Output\Deliver2.csv'  # Replace with your desired output file path
final_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(final_df)

# ***************************

# Read the CSV file into a DataFrame
df = pd.read_csv(output_file)


# Rename columns
rename_dict = {
    '1': 'm01',
    '2': 'm02',
    '3': 'm03',
    '4': 'm04',
    '5': 'm05',
    '6': 'm06',
    '7': 'm07',
    '8': 'm08',
    '9': 'm09',
    '10': 'm10',
    '11': 'm11',
    '12': 'm12'
}

# Check if columns to rename exist
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    # logging.info(f"Columns renamed and data saved to '{updated_file_path}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# ***************************

# Configure logging
# logging.basicConfig(filename='data_processing.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# input_file = r'F:\_BPP\Project\Scraping\Output\Send.csv'

with open(output_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

# uniqueid,yr,kpi_id,genre,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12

create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Deliver' AND xtype='U')
CREATE TABLE Deliver (
    uniqueid INT PRIMARY KEY,
    yr INT,
    kpi_id INT,
    genre NVARCHAR(MAX),
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
# uniqueid,yr,kpi_id,genre,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12
check_sql = "SELECT COUNT(*) FROM Deliver WHERE uniqueid = ?"
column_names = ['uniqueid', 'yr', 'kpi_id', 'genre', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

insert_sql = f"""
INSERT INTO Deliver ({', '.join(column_names)}, create_date)
VALUES ({', '.join(['?'] * len(column_names))}, Getdate())
"""

update_sql = """
UPDATE Deliver
SET yr = ?, kpi_id = ?, genre = ?, m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, m09 = ?, m10 = ?, m11 = ?, m12 = ?, update_date = GETDATE()
WHERE uniqueid = ?
"""


def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except ValueError:
        return None

# ฟังก์ชันเติมค่าเริ่มต้นถ้าไม่มีข้อมูล
def fill_missing_values(row):
    for i in range(4, 15):  # คอลัมน์ m01 ถึง m12
        if row[i] == '' or pd.isna(row[i]):
            row[i] = None  # เปลี่ยนค่าเป็น None หรือ '#N/A' หรือ '0' ตามที่ต้องการ
    return row    

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            for row in data[1:]:
                if len(row) < 17:
                    logging.warning(f"Skipping row due to insufficient columns: {row}")
                    row.extend([None] * (17 - len(row)))  # Fill missing columns with None

                uniqueid = row[0]
                cursor.execute(check_sql, uniqueid)
                exists = cursor.fetchone()[0]

# uniqueid,yr,kpi_id,genre,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12
                if exists:
                    cursor.execute(update_sql, (
                        row[1], 
                        row[2], 
                        row[3], 
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
                        try_float(row[15]), 
                        uniqueid
                    ))
                else:
                    cursor.execute(insert_sql, (
                        uniqueid, 
                        row[1], 
                        row[2], 
                        row[3], 
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
                        try_float(row[15]), 
                    ))

            conn.commit()
    print("เพิ่มหรืออัปเดตข้อมูล Deliver2 เรียบร้อยแล้ว")
except pyodbc.Error as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")
