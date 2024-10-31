import pandas as pd
import pyodbc

# ================================================================================
# 50
# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\50.csv')

# Rename the numeric month columns (1, 2, ..., 12) to match 'm01', 'm02', ..., 'm12'
month_rename_dict = {str(i): f'm{i:02d}' for i in range(1, 13)}
df.rename(columns=month_rename_dict, inplace=True)


# Print the column names to verify the renaming
# print("Renamed Columns in DataFrame:", df.columns)

# Define the numeric columns after renaming
numeric_columns = [f'm{i:02d}' for i in range(1, 13)]

# Convert the numeric columns to floats and handle invalid values by replacing 'None' or NaN with 0.0
for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)

# Preview the data before inserting/updating
# print(df.head())

# Continue with database connection and data insertion/updating as in the previous script

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()

    # Iterate through the DataFrame and insert or update data in SQL Server
    for index, row in df.iterrows():
        cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
        result = cursor.fetchone()[0]

        if result > 0:
            # Update existing record
            sql_update = """
            UPDATE KPI_dtl
            SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
                m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
            WHERE unique_id = ?
            """      
            cursor.execute(sql_update, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                           row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                           row['m11'], row['m12'], row['yr'], row['uniqueid'])
            print(f"Updated row with unique_id = {row['uniqueid']}")
        else:
            # Insert new record
            sql_insert = """
            INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            cursor.execute(sql_insert, row['uniqueid'], row['yr'], row['kpi_id'],  
                           row['m01'], row['m02'], row['m03'], row['m04'], 
                           row['m05'], row['m06'], row['m07'], row['m08'], 
                           row['m09'], row['m10'], row['m11'], row['m12'])
            print(f"Inserted new row with unique_id = {row['uniqueid']}")

    # Commit changes to the database
    conn.commit()

except pyodbc.Error as e:
    print("Error during SQL operation:", e)

finally:
    # Close the cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

# ================================================================================
# 10
import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\10.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
# print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# ================================================================================
# 17

import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\17.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
# print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# ================================================================================
# 18
import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\18.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
# print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# ================================================================================
# 20 - 31

import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\20-31.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
# print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# ================================================================================
# 61-62

import pandas as pd
import pyodbc
from datetime import datetime

# Get the current year
current_year = datetime.now().year

# Define the connection string
server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
constr = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Generate uniqueid
uniqueid61 = current_year * 100 + 61  # Generate an integer unique_id like 202461
uniqueid62 = current_year * 100 + 62
# Define the SQL query for KPI ID 61 update
query_update_61 = f"""
WITH KPI_SUM AS (
    SELECT
        {uniqueid61} AS unique_id,
        61 AS kpi_id,
        {current_year} AS yr,
        SUM(m01) AS [m01],
        SUM(m02) AS [m02],
        SUM(m03) AS [m03],
        SUM(m04) AS [m04],
        SUM(m05) AS [m05],
        SUM(m06) AS [m06],
        SUM(m07) AS [m07],
        SUM(m08) AS [m08],
        SUM(m09) AS [m09],
        SUM(m10) AS [m10],
        SUM(m11) AS [m11],
        SUM(m12) AS [m12]
    FROM [KPI].[dbo].[KPI_dtl]
    WHERE yr = {current_year}  
    AND kpi_id IN (56, 59)
)
UPDATE [KPI].[dbo].[KPI_dtl]
SET 
    m01 = KPI_SUM.m01,
    m02 = KPI_SUM.m02,
    m03 = KPI_SUM.m03,
    m04 = KPI_SUM.m04,
    m05 = KPI_SUM.m05,
    m06 = KPI_SUM.m06,
    m07 = KPI_SUM.m07,
    m08 = KPI_SUM.m08,
    m09 = KPI_SUM.m09,
    m10 = KPI_SUM.m10,
    m11 = KPI_SUM.m11,
    m12 = KPI_SUM.m12,
    update_date = GETDATE()
FROM KPI_SUM
WHERE [KPI].[dbo].[KPI_dtl].unique_id = {uniqueid61} 
  AND [KPI].[dbo].[KPI_dtl].kpi_id = 61 
  AND [KPI].[dbo].[KPI_dtl].yr = {current_year};
"""

# Define the SQL query for KPI ID 62 update
query_update_62 = f"""
WITH KPI_SUM AS (
    SELECT
        {uniqueid62} AS unique_id,
        62 AS kpi_id,
        {current_year} AS yr,
        SUM(m01) AS [m01],
        SUM(m02) AS [m02],
        SUM(m03) AS [m03],
        SUM(m04) AS [m04],
        SUM(m05) AS [m05],
        SUM(m06) AS [m06],
        SUM(m07) AS [m07],
        SUM(m08) AS [m08],
        SUM(m09) AS [m09],
        SUM(m10) AS [m10],
        SUM(m11) AS [m11],
        SUM(m12) AS [m12]
    FROM [KPI].[dbo].[KPI_dtl]
    WHERE yr = {current_year}  
    AND kpi_id IN (55, 60)
)
UPDATE [KPI].[dbo].[KPI_dtl]
SET 
    m01 = KPI_SUM.m01,
    m02 = KPI_SUM.m02,
    m03 = KPI_SUM.m03,
    m04 = KPI_SUM.m04,
    m05 = KPI_SUM.m05,
    m06 = KPI_SUM.m06,
    m07 = KPI_SUM.m07,
    m08 = KPI_SUM.m08,
    m09 = KPI_SUM.m09,
    m10 = KPI_SUM.m10,
    m11 = KPI_SUM.m11,
    m12 = KPI_SUM.m12,
    update_date = GETDATE()
FROM KPI_SUM
WHERE [KPI].[dbo].[KPI_dtl].unique_id = {uniqueid62} 
  AND [KPI].[dbo].[KPI_dtl].kpi_id = 62 
  AND [KPI].[dbo].[KPI_dtl].yr = {current_year};
"""

# Function to execute SQL query with error handling
def execute_query(query_name, query):
    try:
        with pyodbc.connect(constr) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
        print(f"Query '{query_name}' executed successfully.")
    except pyodbc.Error as e:
        print(f"An error occurred while executing '{query_name}': {e}")
        print("Query:", query)  # Print the query for debugging

# Execute the update query
execute_query("query_update_61", query_update_61)
execute_query("query_update_62", query_update_62)

# ================================================================================
# 65
import pyodbc
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\65.csv')

# Check SQL Server connection
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()
    logging.info("Connected to SQL Server successfully.")
except pyodbc.Error as e:
    logging.error("Error connecting to SQL Server: %s", e)
    exit()

# Define numeric columns
numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

# แปลงคอลัมน์ตัวเลขและจัดการค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors='coerce')  # แปลงเป็นตัวเลข
    if df[column].isnull().any():
        logging.warning("Column %s contains invalid values. They will be replaced with 0.0.", column)
        df[column] = df[column].fillna(0.0)  # กำหนดค่าที่เติมกลับไปยังคอลัมน์ต้นฉบับ

# Preview the DataFrame before sending to the database
logging.info("DataFrame preview:\n%s", df.head())

# Insert or update data in SQL Server
try:
    for index, row in df.iterrows():
        cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
        result = cursor.fetchone()[0]
        
        if result > 0:
            # Update existing data
            sql = """
            UPDATE KPI_dtl
            SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
                m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
            WHERE unique_id = ? 
            """      
            cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                           row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                           row['m11'], row['m12'], row['yr'], row['uniqueid'])
            logging.info("Updated record with unique_id: %s", row['uniqueid'])
        else:
            # Insert new data
            sql = """
            INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                           row['m01'], row['m02'], row['m03'], row['m04'], 
                           row['m05'], row['m06'], row['m07'], row['m08'], 
                           row['m09'], row['m10'], row['m11'], row['m12'])
            logging.info("Inserted new record with unique_id: %s", row['uniqueid'])

    # Commit changes
    conn.commit()
    logging.info("Database changes committed successfully.")

except Exception as e:
    logging.error("An error occurred during database operations: %s", e)

finally:
    # Close cursor and connection
    cursor.close()
    conn.close()
    logging.info("Connection to SQL Server closed.")

# ================================================================================
# 66-67
import pyodbc
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\66-67.csv')

# Check SQL Server connection
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()
    logging.info("Connected to SQL Server successfully.")
except pyodbc.Error as e:
    logging.error("Error connecting to SQL Server: %s", e)
    exit()

# Define numeric columns
numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

# แปลงคอลัมน์ตัวเลขและจัดการค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors='coerce')  # แปลงเป็นตัวเลข
    if df[column].isnull().any():
        logging.warning("Column %s contains invalid values. They will be replaced with 0.0.", column)
        df[column] = df[column].fillna(0.0)  # กำหนดค่าที่เติมกลับไปยังคอลัมน์ต้นฉบับ

# Preview the DataFrame before sending to the database
logging.info("DataFrame preview:\n%s", df.head())

# Insert or update data in SQL Server
try:
    for index, row in df.iterrows():
        cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
        result = cursor.fetchone()[0]
        
        if result > 0:
            # Update existing data
            sql = """
            UPDATE KPI_dtl
            SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
                m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
            WHERE unique_id = ? 
            """      
            cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                           row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                           row['m11'], row['m12'], row['yr'], row['uniqueid'])
            logging.info("Updated record with unique_id: %s", row['uniqueid'])
        else:
            # Insert new data
            sql = """
            INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                           row['m01'], row['m02'], row['m03'], row['m04'], 
                           row['m05'], row['m06'], row['m07'], row['m08'], 
                           row['m09'], row['m10'], row['m11'], row['m12'])
            logging.info("Inserted new record with unique_id: %s", row['uniqueid'])

    # Commit changes
    conn.commit()
    logging.info("Database changes committed successfully.")

except Exception as e:
    logging.error("An error occurred during database operations: %s", e)

finally:
    # Close cursor and connection
    cursor.close()
    conn.close()
    logging.info("Connection to SQL Server closed.")

# ================================================================================
# 68

import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\68.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()    

# ================================================================================
# 69
import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\69.csv')

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
            df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
            if df_Reject[column].isnull().any():
                print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
                df_Reject[column] = df_Reject[column].fillna(0.0)

        # ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
        print(df_Reject.head())

        # ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
        for index, row in df_Reject.iterrows():
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
                cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                               row['m01'], row['m02'], row['m03'], row['m04'], 
                               row['m05'], row['m06'], row['m07'], row['m08'], 
                               row['m09'], row['m10'], row['m11'], row['m12'])
                print("create")

        # บันทึกการเปลี่ยนแปลง
        conn.commit()

except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)

# ================================================================================
# 70
    
import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\70.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# ================================================================================
# 72-73

import pyodbc
import pandas as pd

# Load CSV file
df_Reject = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\72-73.csv')


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
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
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
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'],
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()


# ================================================================================
# 77-78

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

# ================================================================================
# 84-90

import pyodbc
import pandas as pd

# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\84-90.csv')

# Access the column using its index (index 4)
dynamic_column_name = df.columns[4]  # Get the name of the column at index 4 (which is 'm09')

# Connect to SQL Server
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

# Insert or update data in SQL Server
for index, row in df.iterrows():
    cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
    result = cursor.fetchone()[0]

    if result > 0:
        # Update data
        sql = f"""
        UPDATE KPI_dtl
        SET {dynamic_column_name} = ?, update_date = GETDATE()
        WHERE unique_id = ? 
        """
        cursor.execute(sql, row[dynamic_column_name], row['uniqueid'])  # Use dynamic column name here
        print(f"Updated {dynamic_column_name} for unique_id {row['uniqueid']}")
    else:
        # Insert new data
        sql = f"""
        INSERT INTO KPI_dtl (unique_id, yr, kpi_id, {dynamic_column_name}, create_date)
        VALUES (?, ?, ?, ?, GETDATE())
        """
        cursor.execute(sql, row['uniqueid'], row['yr'], row['kpi_id'], row[dynamic_column_name])
        print(f"Inserted new row with {dynamic_column_name} for unique_id {row['uniqueid']}")

# Commit changes
conn.commit()

# Close connection
cursor.close()
conn.close()

# ================================================================================
# Update Total
import pandas as pd
import pyodbc

# Define the connection string
server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
constr = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# Define the SQL query
query_update_total_average = """
UPDATE [KPI].[dbo].[KPI_dtl]
SET 
    [total] = 
    (
        COALESCE([m01], 0) + COALESCE([m02], 0) + COALESCE([m03], 0) + 
        COALESCE([m04], 0) + COALESCE([m05], 0) + COALESCE([m06], 0) + 
        COALESCE([m07], 0) + COALESCE([m08], 0) + COALESCE([m09], 0) + 
        COALESCE([m10], 0) + COALESCE([m11], 0) + COALESCE([m12], 0)
    ),
    [average] = 
    COALESCE(
        (
            (
                COALESCE([m01], 0) + COALESCE([m02], 0) + COALESCE([m03], 0) + 
                COALESCE([m04], 0) + COALESCE([m05], 0) + COALESCE([m06], 0) + 
                COALESCE([m07], 0) + COALESCE([m08], 0) + COALESCE([m09], 0) + 
                COALESCE([m10], 0) + COALESCE([m11], 0) + COALESCE([m12], 0)
            ) / 
            NULLIF(
                (
                    CASE WHEN [m01] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m02] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m03] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m04] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m05] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m06] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m07] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m08] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m09] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m10] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m11] IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN [m12] IS NOT NULL THEN 1 ELSE 0 END
                ), 0
            )
        ), 0 -- If the division results in NULL, set it to 0
    )
-- WHERE yr = YEAR(GETDATE());
"""

# Function to execute SQL query with error handling
def execute_query(query_name, query):
    try:
        with pyodbc.connect(constr) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
        print(f"Query '{query_name}' executed successfully.")
    except pyodbc.Error as e:
        print(f"An error occurred while executing '{query_name}': {e}")

# Execute the update query
execute_query("Update Total Average Query", query_update_total_average)

