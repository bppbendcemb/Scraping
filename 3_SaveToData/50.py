import pandas as pd
import pyodbc

# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\2_Calculate\CSV\50.csv')

# Rename the numeric month columns (1, 2, ..., 12) to match 'm01', 'm02', ..., 'm12'
month_rename_dict = {str(i): f'm{i:02d}' for i in range(1, 13)}
df.rename(columns=month_rename_dict, inplace=True)


# Print the column names to verify the renaming
print("Renamed Columns in DataFrame:", df.columns)

# Define the numeric columns after renaming
numeric_columns = [f'm{i:02d}' for i in range(1, 13)]

# Convert the numeric columns to floats and handle invalid values by replacing 'None' or NaN with 0.0
for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0.0)

# Preview the data before inserting/updating
print(df.head())

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
