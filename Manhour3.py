import os
import csv
import pyodbc
import logging

server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'


input_file = os.path.join('step2', 'Output', 'Manhour2.csv')
output_file = os.path.join('step3', 'Output', 'Manhour3.csv')

# Ensure the output directory exists
output_dir = os.path.dirname(output_file)
os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist


with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)


create_table_sql = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Manhour' AND xtype='U')
CREATE TABLE Manhour (
    uniqueid INT PRIMARY KEY,
    yr INT,
    kpi_id INT,
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
    update_date DATETIME,
    create_date DATETIME
)
"""

check_sql = "SELECT COUNT(*) FROM Manhour WHERE uniqueid = ?"
column_names = ['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

insert_sql = f"""
INSERT INTO Manhour ({', '.join(column_names)}, create_date)
VALUES ({', '.join(['?'] * len(column_names))}, Getdate())
"""

# update_sql = """
# UPDATE Manhour
# SET yr = ?, kpi_id = ?,  m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, m09 = ?, m10 = ?, m11 = ?, m12 = ?, update_date = GETDATE()
# WHERE uniqueid = ?
# """

# Create a mapping for month numbers to column names
month_columns = {
    '01': 'm01',
    '02': 'm02',
    '03': 'm03',
    '04': 'm04',
    '05': 'm05',
    '06': 'm06',
    '07': 'm07',
    '08': 'm08',
    '09': 'm09',
    '10': 'm10',
    '11': 'm11',
    '12': 'm12',
}

def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except ValueError:
        return None


try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()

            for row in data[1:]:
                if len(row) < 6:  # Adjust based on your data structure
                    row.extend([None] * (6 - len(row)))  # Fill missing columns with None

                uniqueid = row[5]
                cursor.execute(check_sql, uniqueid)
                exists = cursor.fetchone()[0]

                # Determine the month column to update
                month_value = row[1]  # This is the 'm' column
                column_to_update = month_columns.get(month_value)

                if exists:
                    # Construct the update SQL dynamically
                    update_sql = f"""
                    UPDATE Manhour
                    SET yr = ?, kpi_id = ?, {column_to_update} = ?, update_date = GETDATE()
                    WHERE uniqueid = ?
                    """

                   # yr,m,activityid,catagory,m09,uniqueid
                    cursor.execute(update_sql, (
                        row[0],  # yr
                        row[2],  # kpi_id
                        try_float(row[4]),  # value for the specific month column
                        uniqueid
                    ))
                else:
                    cursor.execute(insert_sql, (
                        uniqueid,
                        row[0],  # yr
                        row[2],  # kpi_id
                        try_float(row[4]),  # value for the specific month column
                        # Add None for the remaining month columns as needed
                        *[None] * 11  # Fill with None for m01 to m12
                    ))

            conn.commit()

    print("Data has been added or updated successfully.")

    # Query updated data from the table to save to CSV
    query = "SELECT * FROM Manhour"
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            # Fetch column names from cursor
            column_names = [column[0] for column in cursor.description]

            # Write the data to the output CSV file
            with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(column_names)  # Write header
                writer.writerows(rows)  # Write data

    print(f"Data saved to {output_file} successfully.")

except pyodbc.Error as e:
    logging.error(f"Database error: {e}")
    print(f"Database error: {e}")

except Exception as e:
    logging.error(f"Error: {e}")
    print(f"Error: {e}")