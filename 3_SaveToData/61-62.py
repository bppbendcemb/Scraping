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
