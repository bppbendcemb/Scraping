import pyodbc
import pandas as pd
from datetime import datetime

# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\77-78.csv')

# ตรวจสอบการเชื่อมต่อกับ SQL Server
try:
    with pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    ) as conn:
        cursor = conn.cursor()

        numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

        # แปลงคอลัมน์ตัวเลขเป็น float และตรวจสอบค่าที่ไม่ถูกต้อง
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)

        # คำนวณ total และ average
        df['total'] = df[numeric_columns].sum(axis=1)
        df['average'] = df[numeric_columns].mean(axis=1)

        # แปลงประเภทของคอลัมน์
        df['uniqueid'] = df['uniqueid'].astype(int)
        df['kpi_id'] = df['kpi_id'].astype(int)
        df['yr'] = df['yr'].astype(int)
        df['total'] = df['total'].astype(float)
        df['average'] = df['average'].astype(float)
        df['target_result'] = 0.0  # กำหนดค่าเริ่มต้นสำหรับ target_result

        # ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
        print(df.head())

        # ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
        for index, row in df.iterrows():
            uniqueid = int(row['uniqueid'])
            cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", uniqueid)
            result = cursor.fetchone()[0]
            
            if result > 0:
                # อัปเดตข้อมูล
                sql = """
                UPDATE KPI_dtl
                SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
                    m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, total = ?, average = ?, target_result = ?, update_date = GETDATE()
                WHERE unique_id = ? 
                """      
                cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                               row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                               row['m11'], row['m12'], row['yr'], row['total'], row['average'], row['target_result'], uniqueid)
                print("update")
            else:
                # แทรกข้อมูลใหม่
                sql = """
                INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, total, average, target_result, create_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                """
                cursor.execute(sql, uniqueid, row['yr'], row['kpi_id'],
                               row['m01'], row['m02'], row['m03'], row['m04'], 
                               row['m05'], row['m06'], row['m07'], row['m08'], 
                               row['m09'], row['m10'], row['m11'], row['m12'], 
                               row['total'], row['average'], row['target_result'])
                print("create")

        # บันทึกการเปลี่ยนแปลง
        conn.commit()

except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)
