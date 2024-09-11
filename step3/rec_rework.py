import csv
import pyodbc

# ฟังก์ชันเพื่อแปลง string ให้เป็น float โดยนำเครื่องหมายจุลภาคออก
def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except ValueError:
        return None  # หากไม่สามารถแปลงเป็น float ได้ ให้คืนค่า None

# กำหนดข้อมูลการเชื่อมต่อกับ SQL Server โดยใช้ Windows Authentication
server = 'c259-003\SQLEXPRESS'
database = 'KPI'

# สร้างการเชื่อมต่อ
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ชื่อไฟล์ CSV ที่ต้องการอ่าน
input_file = r'f:\_BPP\Project\Scraping\step2\Output\Rework_new.csv'

# อ่านข้อมูลจากไฟล์ CSV
with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

# ตรวจสอบว่าตารางในฐานข้อมูลมีคอลัมน์ที่ตรงกันหรือไม่
create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='KPI_Rework' AND xtype='U')
CREATE TABLE KPI_Rework (
    uniqueid INT PRIMARY KEY,
    kpi_id INT,
    yr INT,
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
    total FLOAT,
    average FLOAT,
    target_result FLOAT,
    update_date DATETIME,
    create_date DATETIME
)
"""
cursor.execute(create_table_sql)
conn.commit()

# สร้างคำสั่ง SQL สำหรับการตรวจสอบว่า uniqueid มีอยู่หรือไม่
check_sql = "SELECT COUNT(*) FROM KPI_Rework WHERE uniqueid = ?"

# สร้างคำสั่ง SQL สำหรับการเพิ่มข้อมูล
insert_sql = """
INSERT INTO KPI_Rework (uniqueid, kpi_id, yr, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, Getdate())
"""

# สร้างคำสั่ง SQL สำหรับการอัปเดตข้อมูล
update_sql = """
UPDATE KPI_Rework
SET kpi_id = ?, yr = ?, m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, m09 = ?, m10 = ?, m11 = ?, m12 = ?, update_date = GETDATE()
WHERE uniqueid = ?
"""

# วนลูปผ่านแต่ละแถวของข้อมูลและอัปเดตหรือเพิ่มข้อมูลลงในฐานข้อมูล
for row in data[1:]:  # ข้ามหัวตาราง
    uniqueid = row[0]  # uniqueid จะอยู่ในคอลัมน์แรก

    # ตรวจสอบว่า uniqueid มีอยู่ในฐานข้อมูลหรือไม่
    cursor.execute(check_sql, uniqueid)
    exists = cursor.fetchone()[0]

    # หากมี uniqueid อยู่แล้วให้ทำการอัปเดต
    if exists:
        cursor.execute(update_sql, (
            row[2],  # kpi_id (ข้าม activities)
            row[1],  # yr
            try_float(row[4]),  # m01
            try_float(row[5]),  # m02
            try_float(row[6]),  # m03
            try_float(row[7]),  # m04
            try_float(row[8]),  # m05
            try_float(row[9]),  # m06
            try_float(row[10]),  # m07
            try_float(row[11]),  # m08
            try_float(row[12]),  # m09
            try_float(row[13]),  # m10
            try_float(row[14]),  # m11
            try_float(row[15]),  # m12
            uniqueid  # uniqueid
        ))
    else:
        # หากไม่พบ uniqueid ให้ทำการเพิ่มข้อมูลใหม่
        cursor.execute(insert_sql, (
            uniqueid,  # uniqueid
            row[2],  # kpi_id (ข้าม activities)
            row[1],  # yr
            try_float(row[4]),  # m01
            try_float(row[5]),  # m02
            try_float(row[6]),  # m03
            try_float(row[7]),  # m04
            try_float(row[8]),  # m05
            try_float(row[9]),  # m06
            try_float(row[10]),  # m07
            try_float(row[11]),  # m08
            try_float(row[12]),  # m09
            try_float(row[13]),  # m10
            try_float(row[14]),  # m11
            try_float(row[15]),  # m12
        ))

# บันทึกการเปลี่ยนแปลงในฐานข้อมูล
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

print("เพิ่มหรืออัปเดตข้อมูลเรียบร้อยแล้ว")