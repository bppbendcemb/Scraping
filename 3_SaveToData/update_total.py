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
