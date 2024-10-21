import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pandas as pd
import pyodbc

# กำหนดปีปัจจุบัน
current_year = datetime.now().year
url = 'http://bppnet/energy/report/energy.aspx'

# ส่งคำขอไปยัง URL
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'ctl00_MainContent_GridView1'})
    
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        
        # ดึงข้อมูลจากแต่ละแถวในตาราง
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        # สร้าง DataFrame จาก headers และ rows
        df = pd.DataFrame(rows, columns=headers)

        # สร้างโฟลเดอร์ Output หากยังไม่มี
        folder_Output = 'step1/Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        # สร้างเส้นทางสำหรับไฟล์ CSV
        file_path = os.path.join(folder_Output, 'Kwh.csv')    

        # บันทึกข้อมูลลงในไฟล์ CSV
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("Save To : Kwh.csv")
    else:
        print("No Table found on the page.")
else:
    print(f"Page Not Found: {response.status_code}")

# ---------------------------------------------------------------------------------------

# โหลดข้อมูลจากไฟล์ CSV
df = pd.read_csv(file_path)

# ดึงค่า KWH จากคอลัมน์ 'KWH'
kwh_values = df['KWH'].tolist()

# แปลงค่าเป็น float และกรองค่าว่าง (หากค่าว่างให้ใช้ pd.NA)
kwh_values = [float(value.replace(',', '')) if isinstance(value, str) and value else pd.NA for value in kwh_values]

# ตรวจสอบว่ามี 12 เดือน ถ้าขาดให้เติม pd.NA
while len(kwh_values) < 12:
    kwh_values.append(pd.NA)

# สร้าง DataFrame ใหม่ที่มีเดือนเป็นหัวตาราง
months = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
kwh_df = pd.DataFrame(columns=['uniqueid', 'yr', 'kpi_id'] + months)

# เพิ่มข้อมูลลงใน DataFrame ใหม่
kpi_id = 66  # ค่า kpi_id คงที่
uniqueid = str(current_year) + str(kpi_id)  # สร้าง uniqueid

# สร้าง dictionary สำหรับแถวข้อมูลใหม่
new_row = {
    'uniqueid': uniqueid,
    'yr': current_year,
    'kpi_id': kpi_id,
    **dict(zip(months, kwh_values))
}

# ใช้ pd.concat แทน kwh_df.append
kwh_df = pd.concat([kwh_df, pd.DataFrame([new_row])], ignore_index=True)

# แสดงผลลัพธ์
print(kwh_df)

folder_Output = 'step2/Output'
if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)
# สร้างเส้นทางสำหรับไฟล์ CSV
file_path = os.path.join(folder_Output, 'Kwh2.csv')    

# บันทึก DataFrame ลงในไฟล์ CSV
# kwh_df.to_csv(file_path, index=False)  # บันทึกทับไฟล์เดิม
kwh_df.to_csv(file_path, index=False)

# ---------------------------------------------------------------------------------------

# ตรวจสอบการเชื่อมต่อกับ SQL Server
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)
    exit()

cursor = conn.cursor()

# ตรวจสอบและสร้างตารางถ้ายังไม่มี
# try:
#     cursor.execute("""
#     IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Kwh')
#     BEGIN
#         CREATE TABLE Kwh (
#             unique_id INT,       
#             yr INT,
#             kpi_id INT,
#             m01 DECIMAL(18, 2),
#             m02 DECIMAL(18, 2),
#             m03 DECIMAL(18, 2),
#             m04 DECIMAL(18, 2),
#             m05 DECIMAL(18, 2),
#             m06 DECIMAL(18, 2),
#             m07 DECIMAL(18, 2),
#             m08 DECIMAL(18, 2),
#             m09 DECIMAL(18, 2),
#             m10 DECIMAL(18, 2),
#             m11 DECIMAL(18, 2),
#             m12 DECIMAL(18, 2),
#             update_date DATETIME,
#             create_date DATETIME
#         )
#     END
#     """)
#     conn.commit()

# except pyodbc.Error as e:
#     print("Error checking or creating table:", e)

numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
# แปลงคอลัมน์ตัวเลขเป็น float และตรวจสอบค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    kwh_df[column] = pd.to_numeric(kwh_df[column], errors='coerce')
    if kwh_df[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        kwh_df[column] = kwh_df[column].fillna(0.0)


# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(kwh_df.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in kwh_df.iterrows():
    cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
    result = cursor.fetchone()[0]
    
    if result > 0:
        # อัปเดตข้อมูล
        sql = """
        UPDATE KPI_dtl
        SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
            m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
        WHERE unique_id = ? 
        """      
        cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                       row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                       row['m11'], row['m12'], row['yr'], row['uniqueid'])
        print("update")
    else:
        # แทรกข้อมูลใหม่
        sql = """
        INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        cursor.execute(sql, row['uniqueid'], row['yr'], 66,  # เปลี่ยน row['id'] เป็น 66
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()
