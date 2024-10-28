import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pyodbc
import pandas as pd

current_year = datetime.now().year
url = 'http://bppnet/energy/report/p2summary.aspx'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView2'})
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

        file_path = os.path.join(folder_Output, 'Lpg.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("Save To : Lpg.csv")
    else:
        print("No Table")
else:
    print(f"Page Not Fauls: {response.status_code}")

# ---------------------------------------------------------------------------------------

# โหลดข้อมูลจากไฟล์ CSV
df = pd.read_csv(file_path)

# ดึงค่า ยอดรวมการใช้ LPG(กก.) จากคอลัมน์ 'ยอดรวมการใช้ LPG(กก.)'
lpg_values = df['ยอดรวมการใช้ LPG(กก.)'].tolist()

# แปลงค่าเป็น float และกรองค่าว่าง (หากค่าว่างให้ใช้ pd.NA)
lpg_values = [float(value.replace(',', '')) if isinstance(value, str) and value else pd.NA for value in lpg_values]

# ตรวจสอบว่ามี 12 เดือน ถ้าขาดให้เติม pd.NA
while len(lpg_values) < 12:
    lpg_values.append(pd.NA)

# สร้าง DataFrame ใหม่ที่มีเดือนเป็นหัวตาราง
months = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
lpg_df = pd.DataFrame(columns=['uniqueid', 'yr', 'kpi_id'] + months)

# เพิ่มข้อมูลลงใน DataFrame ใหม่
kpi_id = 68  # ค่า kpi_id คงที่
uniqueid = str(current_year) + str(kpi_id)  # สร้าง uniqueid

# สร้าง dictionary สำหรับแถวข้อมูลใหม่
new_row = {
    'uniqueid': uniqueid,
    'yr': current_year,
    'kpi_id': kpi_id,
    **dict(zip(months, lpg_values))
}

# ใช้ pd.concat แทน lpg_df.append
lpg_df = pd.concat([lpg_df, pd.DataFrame([new_row])], ignore_index=True)

# แสดงผลลัพธ์
print(lpg_df)

folder_Output = 'step2/Output'
if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)
# สร้างเส้นทางสำหรับไฟล์ CSV
file_path = os.path.join(folder_Output, 'Lpg2.csv')    

# บันทึก DataFrame ลงในไฟล์ CSV
# lpg_df.to_csv(file_path, index=False)  # บันทึกทับไฟล์เดิม
lpg_df.to_csv(file_path, index=False)

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

numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
# แปลงคอลัมน์ตัวเลขเป็น float และตรวจสอบค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    lpg_df[column] = pd.to_numeric(lpg_df[column], errors='coerce')
    if lpg_df[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        lpg_df[column] = lpg_df[column].fillna(0.0)


# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(lpg_df.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in lpg_df.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], 68,  # เปลี่ยน row['id'] เป็น 68
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()
