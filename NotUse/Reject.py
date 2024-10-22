import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pandas as pd
import logging
import pyodbc

# step 1

current_year = datetime.now().year
url = f'http://bppnet/qm/report/ccrlst.aspx?ktype=yr&key={current_year}'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_datagrid1'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'Reject1.csv')
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
           writer = csv.writer(file)
           writer.writerow(headers)
           writer.writerows(rows)
        print("บันทึกไฟล์ชื่อ : Reject1.csv")
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")       
#______________________________________________________

# step 2

# จัดเรียงข้อมูลให้ครบ 12 เดือน
# ไม่เอาสถานะ 'ยกเลิก'
# รวมข้อมูล เดือนโดย group รวมกัน

# Define file paths
input_file = os.path.join(folder_Output, 'Reject1.csv')
output_file = os.path.join(folder_Output, 'Reject2.csv')

# Read the CSV file
df = pd.read_csv(input_file)

# Convert 'วันที่เอกสาร' column to datetime format in Reject DataFrame
df['วันที่เอกสาร'] = pd.to_datetime(df['วันที่เอกสาร'], format='%d/%m/%Y')

# Clean and convert 'Reject' column from text to numbers
df['Reject'] = df['Reject'].astype(str).str.replace(',', '').str.strip()
df['Reject'] = pd.to_numeric(df['Reject'], errors='coerce')

# เปลี่ยนชื่อหลายคอลัมน์ใน DataFrame
rename_columns = {
    'วันที่เอกสาร': 'date',  # เปลี่ยนชื่อเป็น 'date'
    'สถานะเอกสาร': 'status',  # เปลี่ยนชื่อเป็น 'status'
}

df.rename(columns=rename_columns, inplace=True)

# เพิ่มคอลัมน์ 'm' และ 'yr'
df['m'] = df['date'].dt.month
df['yr'] = df['date'].dt.year

# เลือกเฉพาะคอลัมน์ที่ต้องการใช้งาน
selected_columns = ['yr', 'm', 'status', 'Reject']
df_selected = df[selected_columns]

# ลบแถวที่ status = 'ยกเลิก'
df_filtered = df_selected[df_selected['status'] != 'ยกเลิก']

# เลือกเฉพาะคอลัมน์ 'yr', 'm', และ 'Reject'
selected_columns2 = ['yr', 'm', 'Reject']
df_selected2 = df_filtered[selected_columns2]

# Group by 'yr' และ 'm' และรวมค่าในคอลัมน์ 'Reject'
df_grouped = df_selected2.groupby(['yr', 'm'], as_index=False)['Reject'].sum()

# Pivot table: เปลี่ยนค่าของ 'm' เป็นคอลัมน์ และแสดงผลรวมของ 'Reject'
df_pivot = df_grouped.pivot_table(index='yr', columns='m', values='Reject', aggfunc='sum').reset_index()

# Create a DataFrame with all months (1-12)
all_months = pd.DataFrame(0, index=range(len(df_pivot)), columns=range(1, 13))  # Create columns for months 1-12
all_months['yr'] = df_pivot['yr']  # Add 'yr' column from df_pivot

# Fill in the values from df_pivot
for month in range(1, 13):
    if month in df_pivot.columns:
        all_months[month] = df_pivot[month].fillna(0)  # Fill NaN with 0

# Reorder columns to move 'yr' to the leftmost position
all_months = all_months[['yr'] + [col for col in all_months.columns if col != 'yr']]

# Rename columns
rename_dict = {
    1: 'm01',
    2: 'm02',
    3: 'm03',
    4: 'm04',
    5: 'm05',
    6: 'm06',
    7: 'm07',
    8: 'm08',
    9: 'm09',
    10: 'm10',
    11: 'm11',
    12: 'm12'
}

kpi_id = [10]
all_months.insert(loc=1, column='kpi_id', value=kpi_id)


# Check if columns to rename exist in all_months
if set(rename_dict.keys()).issubset(all_months.columns):
    all_months.rename(columns=rename_dict, inplace=True)
    
else:
    print("One or more columns to rename do not exist in all_months")

# บันทึก DataFrame ที่ได้ไปยังไฟล์ CSV
all_months.to_csv(output_file, encoding='utf-8-sig', index=False)
print("บันทึกไฟล์ชื่อ: Reject2.csv หลังจากเพิ่มคอลัมน์เดือนครบ 12 เดือน")

#______________________________________________________

#step 3

# Define file paths
input_file = os.path.join(folder_Output, 'Reject2.csv')
output_file = os.path.join(folder_Output, 'Reject3.csv')
input_file_deliver = os.path.join(folder_Output, 'Deliver2.csv')

# Read the CSV files
df_Reject2 = pd.read_csv(input_file)
df_Deliver2 = pd.read_csv(input_file_deliver)

print("Reject2 DataFrame:")
print(df_Reject2)
print("Deliver2 DataFrame:")
print(df_Deliver2)

# Union of the two DataFrames
df_union = pd.concat([df_Reject2, df_Deliver2]).drop_duplicates().reset_index(drop=True)

# Filter for specific kpi_id
filtered_df = df_union[df_union['kpi_id'].isin([10, 50])]

# Save the filtered DataFrame to CSV
filtered_df.to_csv(output_file, encoding='utf-8-sig', index=False)

# Get the values for kpi_id=50
kpi_50 = filtered_df[filtered_df['kpi_id'] == 50]

# Create a list to store results
results_list = []
yr_value = filtered_df['yr'].unique()[0]
# Calculate the ratio for each month
for month in range(1, 13):
    try:
        kpi_10_value = filtered_df[filtered_df['kpi_id'] == 10][f'm{month:02}'].values[0]  # Use m01, m02, ...
        kpi_50_value = kpi_50[f'm{month:02}'].values[0]  # Value for kpi_id=50 for that month

        # Calculate the ratio
        if kpi_50_value != 0:  # Check to avoid division by zero
            ratio = (kpi_10_value * 1000000) / kpi_50_value
        else:
            ratio = None  # If kpi_50_value is 0, set ratio to None

        # Add the data to the results list
        results_list.append({'yr': yr_value, 'kpi_id': 10, 'month': month, 'ratio': ratio})

    except IndexError:
        print(f"No data for month {month} for kpi_id 10 or 50.")
        continue

# Convert results list to DataFrame
result = pd.DataFrame(results_list)

# Add 'yr' and 'kpi_id' columns
result['yr'] = yr_value
result['kpi_id'] = 10
# Create 'uniqueid' column by concatenating 'yr' and 'kpi_id'
result['uniqueid'] = result['yr'].astype(str) + result['kpi_id'].astype(str)

# Reorder columns to place 'yr', 'kpi_id', and 'uniqueid' at the front
result = result[['uniqueid', 'yr', 'kpi_id', 'month', 'ratio']]

# Pivot the DataFrame to make it horizontal
result_pivot = result.pivot(index=['uniqueid','yr', 'kpi_id'], columns='month', values='ratio').reset_index()

# Rename columns for clarity
result_pivot.columns.name = None  # Remove the name of the columns
result_pivot.columns = [f'm{int(col):02}' if isinstance(col, int) else col for col in result_pivot.columns]

# Display the result DataFrame
print("Result DataFrame (Horizontal):")
print(result_pivot)

# Optional: Save the horizontal result DataFrame to CSV

result_pivot.to_csv(output_file, encoding='utf-8-sig', index=False)

#__________________________________________________________

#step 4 บันทึกลง sql server

server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

with open(output_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Reject' AND xtype='U')
CREATE TABLE Reject (
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

check_sql = "SELECT COUNT(*) FROM Reject WHERE uniqueid = ?"
column_names = ['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

insert_sql = f"""
INSERT INTO Reject ({', '.join(column_names)}, create_date)
VALUES ({', '.join(['?'] * len(column_names))}, Getdate())
"""

update_sql = """
UPDATE Reject
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
    for i in range(3, 14):  # คอลัมน์ m01 ถึง m12
        if row[i] == '' or pd.isna(row[i]):
            row[i] = None  # เปลี่ยนค่าเป็น None หรือ '#N/A' หรือ '0' ตามที่ต้องการ
    return row    

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            for row in data[1:]:
                if len(row) < 16:
                    logging.warning(f"Skipping row due to insufficient columns: {row}")
                    row.extend([None] * (16 - len(row)))  # Fill missing columns with None

                uniqueid = row[0]
                cursor.execute(check_sql, uniqueid)
                exists = cursor.fetchone()[0]

# uniqueid,yr,kpi_id,genre,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12
                if exists:
                    cursor.execute(update_sql, (
                        row[1], 
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
    print("เพิ่มหรืออัปเดตข้อมูล Reject เรียบร้อยแล้ว")
except pyodbc.Error as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")
