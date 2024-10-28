import pyodbc

# Connection string
conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=c259-003\SQLEXPRESS;'
    r'DATABASE=KPI;'
    r'Trusted_Connection=yes;'
)

# Establish connection
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

def execute_query(query, params=None):
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        print("Query executed successfully.")
    except pyodbc.Error as e:
        print(f"Error executing query: {str(e)}")
        conn.rollback()

def create_reworklostnew_table(source_table):
    create_query = f"""
    IF OBJECT_ID('ReworkLostNew', 'U') IS NOT NULL
        DROP TABLE ReworkLostNew;
    
    SELECT *
    INTO ReworkLostNew
    FROM {source_table}
    """
    execute_query(create_query)

def display_sample_data(table_name):
    query = f"SELECT TOP 5 * FROM {table_name}"
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    
    print(f"\nSample data from {table_name}:")
    print(", ".join(columns))
    for row in rows:
        print(row)

try:
    # List all tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
    tables = [table[0] for table in cursor.fetchall()]
    
    print("Tables in your KPI database:")
    for i, table in enumerate(tables, 1):
        print(f"{i}. {table}")

    # Get user input
    source_table_index = int(input("\nEnter the number of the source table: ")) - 1
    source_table = tables[source_table_index]

    # Create ReworkLostNew table
    create_reworklostnew_table(source_table)
    print(f"Table 'ReworkLostNew' created successfully from '{source_table}'.")

    # Display sample data from source table
    display_sample_data(source_table)

    # Display sample data from new ReworkLostNew table
    display_sample_data('ReworkLostNew')

    print("\nThe ReworkLostNew table has been created with data from the source table.")
    print("You can now use this table for further operations or updates.")

except pyodbc.Error as e:
    print(f"Error: {str(e)}")

finally:
    cursor.close()
    conn.close()
    