import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pandas as pd
import logging
import pyodbc

# step1: scraping by id
url = 'http://bppnet/qm/report/ncstatus.aspx'
response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'ctl00_MainContent_GridView1'}) # ctl00_MainContent_GridView1
    
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        # Create a pandas DataFrame from the extracted data
        df = pd.DataFrame(rows, columns=headers)

        folder_Output = os.path.join('step1', 'Output')
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'CustFeedbackRequest1.csv')
        
        # Save DataFrame to a CSV file
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"บันทึกไฟล์ชื่อ: CustFeedbackRequest1.csv")
    else:
        print("No Table Found")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

#-----------------------------------------------------

# step2
input_file = os.path.join('step1', 'Output', 'CustFeedbackRequest1.csv')
output_file = os.path.join('step1', 'Output', 'CustFeedbackRequest2.csv')

# Read the CSV file
df = pd.read_csv(input_file)

# Rename columns
rename_dict = {
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

# Rename columns if they exist
df.rename(columns=rename_dict, inplace=True)

# คอลัมน์ใน DataFrame
columns = ['uniqueid', 'yr', 'kpi_id', 'ลำดับ', 'กิจกรรม', 'เปิดปีที่แล้ว', 'ปิดของปีที่แล้ว', 'เปิดปีนี้', 'ปิดปีนี้', 'เปิดอยู่', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

# Ensure all required columns are present
for col in columns:
    if col not in df.columns:
        df[col] = None  # Add missing columns with None values

# เพิ่ม column year
df['yr'] = pd.Timestamp.now().year  # หรือเปลี่ยนเป็นปีที่ต้องการ

# เปลี่ยนค่า kpi_id ตามเงื่อนไขที่กำหนด
df.loc[df['ลำดับ'] == 101, 'kpi_id'] = 77
df.loc[df['ลำดับ'] == 102, 'kpi_id'] = 78

# แปลงค่า kpi_id ที่ไม่ใช่ตัวเลขเป็น NaN
df['kpi_id'] = pd.to_numeric(df['kpi_id'], errors='coerce')

# กรณีที่ kpi_id เป็น NaN ให้กำหนดเป็น -1
df['kpi_id'] = df['kpi_id'].fillna(-1).astype(int)  # ใช้ -1 แทน NaN

# กรองข้อมูลเฉพาะลำดับ 101 และ 102
df_filtered = df.loc[df['ลำดับ'].isin([101, 102])]

# ถ้าไม่มีข้อมูลให้แสดงข้อความ
if df_filtered.empty:
    print("ไม่มีข้อมูลสำหรับลำดับ 101 และ 102")
else:
    # คำนวณ uniqueid ใหม่ใน df_filtered โดยใช้ .loc
    df_filtered.loc[:, 'uniqueid'] = df_filtered['yr'].astype(str) + df_filtered['kpi_id'].astype(str)

    # Define the new column order
    new_order = [
        'uniqueid', 'yr', 'kpi_id', 'ลำดับ', 'กิจกรรม', 
        'เปิดปีที่แล้ว', 'ปิดของปีที่แล้ว', 'เปิดปีนี้', 
        'ปิดปีนี้', 'เปิดอยู่', 'm01', 'm02', 'm03', 
        'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 
        'm10', 'm11', 'm12', 'ยังไม่ตอบสาเหตุ', 'ปิดปีนี้'
    ]

    # Reindex the DataFrame to match the new column order
    df_filtered = df_filtered.reindex(columns=new_order)

    # บันทึก DataFrame ที่ได้ไปยังไฟล์ CSV
    df_filtered.to_csv(output_file, encoding='utf-8-sig', index=False)
    print("บันทึกไฟล์ชื่อ: CustFeedbackRequest2.csv")

#-------------------------------------

server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

with open(output_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CustFeedbackRequest' AND xtype='U')
CREATE TABLE CustFeedbackRequest(
    uniqueid INT PRIMARY KEY,
    yr INT,
    kpi_id INT,
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

check_sql = "SELECT COUNT(*) FROM CustFeedbackRequest WHERE uniqueid = ?"
column_names = ['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

insert_sql = f"""
INSERT INTO CustFeedbackRequest ({', '.join(column_names)}, create_date)
VALUES ({', '.join(['?'] * len(column_names))}, Getdate())
"""

update_sql = """
UPDATE CustFeedbackRequest
SET yr = ?, kpi_id = ?, m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, m09 = ?, m10 = ?, m11 = ?, m12 = ?, update_date = GETDATE()
WHERE uniqueid = ?
"""

def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except ValueError:
        return None

# ฟังก์ชันเติมค่าเริ่มต้นถ้าไม่มีข้อมูล
def fill_missing_values(row):
    for i in range(10, 22):  # คอลัมน์ m01 ถึง m12
        if row[i] == '' or pd.isna(row[i]):
            row[i] = None  # เปลี่ยนค่าเป็น None หรือ '#N/A' หรือ '0' ตามที่ต้องการ
    return row    

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            for row in data[1:]:
                if len(row) < 23:
                    logging.warning(f"Skipping row due to insufficient columns: {row}")
                    row.extend([None] * (23 - len(row)))  # Fill missing columns with None

                uniqueid = row[0]
                cursor.execute(check_sql, uniqueid)
                exists = cursor.fetchone()[0]

# uniqueid,yr,kpi_id,genre,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12
                if exists:
                    cursor.execute(update_sql, (
                        row[1], 
                        row[2], 
                        try_float(row[10]),
                        try_float(row[11]), 
                        try_float(row[12]), 
                        try_float(row[13]), 
                        try_float(row[14]),
                        try_float(row[15]), 
                        try_float(row[16]), 
                        try_float(row[17]), 
                        try_float(row[18]), 
                        try_float(row[19]),
                        try_float(row[20]), 
                        try_float(row[21]), 
                        uniqueid
                    ))
                else:
                    cursor.execute(insert_sql, (
                        uniqueid, 
                        row[1], 
                        row[2], 
                        try_float(row[10]),
                        try_float(row[11]), 
                        try_float(row[12]), 
                        try_float(row[13]), 
                        try_float(row[14]),
                        try_float(row[15]), 
                        try_float(row[16]), 
                        try_float(row[17]), 
                        try_float(row[18]), 
                        try_float(row[19]),
                        try_float(row[20]), 
                        try_float(row[21]), 
                    ))

            conn.commit()
    print("เพิ่มหรืออัปเดตข้อมูล CustFeedbackRequest เรียบร้อยแล้ว")
except pyodbc.Error as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")


