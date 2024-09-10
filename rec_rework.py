import csv
import pyodbc

# กำหนดข้อมูลการเชื่อมต่อกับ SQL Server โดยใช้ Windows Authentication
server = 'c259-003\SQLEXPRESS'
database = 'KPI'

# สร้างการเชื่อมต่อ
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ชื่อไฟล์ CSV ที่ต้องการอ่าน
input_file = r'f:\_BPP\Project\Scraping\step2\Output\Rework_new.csv'
output_file = 'Rework_new.csv'

# อ่านข้อมูลจากไฟล์ CSV
with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

# ตรวจสอบว่าตารางในฐานข้อมูลมีคอลัมน์ที่ตรงกันหรือไม่
create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='KPI_Rework' AND xtype='U')
CREATE TABLE KPI_Rework (
    yr VARCHAR(50),
    activities VARCHAR(255),
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
    kpi_id VARCHAR(10),
    uniqueid VARCHAR(50) PRIMARY KEY
)
"""
cursor.execute(create_table_sql)
conn.commit()

# สร้างคำสั่ง SQL สำหรับการเพิ่มข้อมูล
insert_sql = """
INSERT INTO KPI_Rework (ปี, รายการ, ม.ค., ก.พ., มี.ค., เม.ย., พ.ค., มิ.ย., ก.ค., ส.ค., ก.ย., ต.ค., พ.ย., ธ.ค., kpi_id, uniqueid)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# สร้างคำสั่ง SQL สำหรับการอัปเดตข้อมูล
update_sql = """
UPDATE KPI_Rework
SET ปี = ?, รายการ = ?, ม.ค. = ?, ก.พ. = ?, มี.ค. = ?, เม.ย. = ?, พ.ค. = ?, มิ.ย. = ?, ก.ค. = ?, ส.ค. = ?, ก.ย. = ?, ต.ค. = ?, พ.ย. = ?, ธ.ค. = ?, kpi_id = ?
WHERE uniqueid = ?
"""

# วนลูปผ่านแต่ละแถวของข้อมูลและอัปเดตหรือเพิ่มข้อมูลลงในฐานข้อมูล
for row in data[1:]:  # ข้ามหัวตาราง
    ปี = row[0]
    รายการ = row[1]
    uniqueid = f"{ปี}{row[2]}"  # รวมปีและ kpi_id เพื่อสร้าง
