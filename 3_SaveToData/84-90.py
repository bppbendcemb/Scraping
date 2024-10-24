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
