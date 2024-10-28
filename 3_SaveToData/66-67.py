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
