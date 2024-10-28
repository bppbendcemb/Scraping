import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pyodbc
import pandas as pd

current_year = datetime.now().year
url = f'http://bppnet/report/pd/pdrwsumyr.aspx?yr{current_year}'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
 # สร้าง DataFrame จาก headers และ rows
        df = pd.DataFrame(rows, columns=headers)

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'CostRework.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("Save To : CostRework.csv")
    else:
        print("No Table")
else:
    print(f"Page Not Fauls: {response.status_code}")

# -------------------------------------------------------------------------------------

input_file = os.path.join(folder_Output, 'CostRework.csv')
# Read the CSV file
# df = pd.read_csv(input_file)

# Rename columns
rename_columns = {
    'ม.ค.': 'm01',
    'ก.พ.': 'm02',
    'มี.ค.': 'm03',
    'เม.ย.': 'm04',
    'พ.ค.': 'm05',
    'มิ.ย.': 'm06',
    'ก.ค.': 'm07',
    'ส.ค.': 'm08',
    'ก.ย.': 'm09',
    'ต.ค.':  'm10',
    'พ.ย.':  'm11',
    'ธ.ค.':  'm12'
}

df.rename(columns=rename_columns, inplace=True)

# ดึงแถวที่มี 'รวม:' ในคอลัมน์แรก
# df_filtered = df[df.iloc[:, 0].str.contains('รวม:', na=False)]
df_filtered = df[df.iloc[:, 0].str.contains('รวม:', na=False)].copy()  # ใช้ .copy() เพื่อหลีกเลี่ยงคำเตือน

# เพิ่มคอลัมน์ year และ id

df_filtered['yr'] = current_year  # กำหนดปี
df_filtered['id'] = 65      # กำหนดค่า id = 65

# แปลงค่า 'yr' และ 'id' เป็น string และทำการต่อ string
df_filtered['uniqueid'] = df_filtered['yr'].astype(str) + df_filtered['id'].astype(str)
# df_filtered['uniqueid'] = (df_filtered['yr'].astype(str) + df_filtered['id'].astype(str)).astype(int)


# ลบคอลัมน์ 'สาเหตุ' และ 'รวม (บาท)' ซึ่งคือคอลัมน์แรกและที่สอง
df_filtered = df_filtered.drop(['สาเหตุ', 'รวม (บาท)'], axis=1)

# ย้ายคอลัมน์ 'year' และ 'id' ไปทางซ้ายสุด
cols = ['uniqueid', 'yr', 'id'] + [col for col in df_filtered.columns if col not in ['uniqueid', 'yr', 'id']]
df_filtered = df_filtered[cols]

folder_Output = 'step2\Output'
if not os.path.exists(folder_Output):
    os.makedirs(folder_Output)


file_path = os.path.join(folder_Output, 'CostRework2.csv')
df_filtered.to_csv(file_path, index=False, encoding='utf-8-sig')
print("Save To : CostRework2.csv")

# -------------------------------------------------------------------------------------------------------

# การเชื่อมต่อกับ SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=c259-003\\SQLEXPRESS;'
    'DATABASE=KPI;'
    'Trusted_Connection=yes;'
)

cursor = conn.cursor()

# ตัวอย่าง DataFrame ที่ต้องการอัปเดต
# df_filtered = pd.read_csv('path_to_csv/CostRework2.csv')

# ลบจุลภาคออกจากคอลัมน์ตัวเลข
numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
df_filtered[numeric_columns] = df_filtered[numeric_columns].replace({',': ''}, regex=True)

# แปลงคอลัมน์ตัวเลขเป็นชนิด float
df_filtered[numeric_columns] = df_filtered[numeric_columns].apply(pd.to_numeric, errors='coerce')

# ตรวจสอบค่าว่าง (NaN) และแทนที่ด้วยค่าเริ่มต้น เช่น 0.0
df_filtered[numeric_columns] = df_filtered[numeric_columns].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_filtered.head())

# ตรวจสอบว่าตารางมีอยู่หรือไม่ ถ้าไม่มี ให้สร้างใหม่
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CostSummary')
BEGIN
    CREATE TABLE CostSummary (
        unique_id INT,       
        yr INT,
        kpi_id INT,
        m01 DECIMAL(18, 2),
        m02 DECIMAL(18, 2),
        m03 DECIMAL(18, 2),
        m04 DECIMAL(18, 2),
        m05 DECIMAL(18, 2),
        m06 DECIMAL(18, 2),
        m07 DECIMAL(18, 2),
        m08 DECIMAL(18, 2),
        m09 DECIMAL(18, 2),
        m10 DECIMAL(18, 2),
        m11 DECIMAL(18, 2),
        m12 DECIMAL(18, 2),
        update_date DATETIME,
        create_date DATETIME
    )
END
""")
conn.commit()

# อัปเดตข้อมูลในตาราง 'CostSummary'
for index, row in df_filtered.iterrows():
    # ตรวจสอบว่ามี id อยู่แล้วหรือไม่
    # cursor.execute("SELECT COUNT(*) FROM CostSummary WHERE uniqueid = ?", row['uniqueid'])
    cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])

    result = cursor.fetchone()[0]
    
    if result > 0:
        # ถ้า id มีอยู่แล้ว ให้ทำการอัปเดตข้อมูล
        sql = """
        UPDATE KPI_dtl
        SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
            m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
        WHERE unique_id = ?
        """      
        cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                       row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                       row['m11'], row['m12'], row['yr'], row['uniqueid'])  # แก้ไขตรงนี้เพิ่ม uniqueid
        print("update")
    else:
        # ถ้า id ไม่มีอยู่ ให้แทรกข้อมูลใหม่
        sql = """
        INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        cursor.execute(sql, row['uniqueid'], row['yr'], row['id'], row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], row['m09'], 
                       row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()