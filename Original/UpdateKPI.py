import pandas as pd
import pyodbc

# Define the connection string
server = 'c259-003\\SQLEXPRESS'
database = 'KPI'
constr = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

# SQL queries
query_backup = """
IF OBJECT_ID('dbo.KPI_dtl_backup', 'U') IS NOT NULL 
    DROP TABLE dbo.KPI_dtl_backup;

SELECT * 
INTO KPI_dtl_backup
FROM [dbo].[KPI_dtl];
"""

query_update_rework = """
UPDATE B
SET
    B.[kpi_id] = A.[id],
    B.[yr] = A.[yr],
    B.[m01] = A.[m01],
    B.[m02] = A.[m02],
    B.[m03] = A.[m03],
    B.[m04] = A.[m04],
    B.[m05] = A.[m05],
    B.[m06] = A.[m06],
    B.[m07] = A.[m07],
    B.[m08] = A.[m08],
    B.[m09] = A.[m09],
    B.[m10] = A.[m10],
    B.[m11] = A.[m11],
    B.[m12] = A.[m12],
    B.[update_date] = GETDATE()
FROM [KPI_dtl] B
JOIN [ReworkLost] A ON A.activityid = B.unique_id
WHERE B.kpi_id IN (17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
"""

query_update_deliver = """
UPDATE B
SET
    B.[kpi_id] = A.kpi_id,
    B.[yr] = A.[yr],
    B.[m01] = A.[m01],
    B.[m02] = A.[m02],
    B.[m03] = A.[m03],
    B.[m04] = A.[m04],
    B.[m05] = A.[m05],
    B.[m06] = A.[m06],
    B.[m07] = A.[m07],
    B.[m08] = A.[m08],
    B.[m09] = A.[m09],
    B.[m10] = A.[m10],
    B.[m11] = A.[m11],
    B.[m12] = A.[m12],
    B.[update_date] = GETDATE()
FROM [KPI_dtl] B
JOIN [dbo].[Deliver] A ON A.uniqueid = B.unique_id
WHERE B.kpi_id = 50;
"""

query_update_reject = """
UPDATE B
SET
    B.[kpi_id] = A.kpi_id,
    B.[yr] = A.[yr],
    B.[m01] = A.[m01],
    B.[m02] = A.[m02],
    B.[m03] = A.[m03],
    B.[m04] = A.[m04],
    B.[m05] = A.[m05],
    B.[m06] = A.[m06],
    B.[m07] = A.[m07],
    B.[m08] = A.[m08],
    B.[m09] = A.[m09],
    B.[m10] = A.[m10],
    B.[m11] = A.[m11],
    B.[m12] = A.[m12],
    B.[update_date] = GETDATE()
FROM [KPI_dtl] B
JOIN [dbo].[Reject] A ON A.uniqueid = B.unique_id
WHERE B.kpi_id = 10;
"""

query_update_CustFeedbackRequest = """
UPDATE B
SET
    B.[kpi_id] = A.kpi_id,
    B.[yr] = A.[yr],
    B.[m01] = A.[m01],
    B.[m02] = A.[m02],
    B.[m03] = A.[m03],
    B.[m04] = A.[m04],
    B.[m05] = A.[m05],
    B.[m06] = A.[m06],
    B.[m07] = A.[m07],
    B.[m08] = A.[m08],
    B.[m09] = A.[m09],
    B.[m10] = A.[m10],
    B.[m11] = A.[m11],
    B.[m12] = A.[m12],
    B.[update_date] = GETDATE()
FROM [KPI_dtl] B
JOIN [dbo].[CustFeedbackRequest] A 
ON A.uniqueid = B.unique_id
WHERE B.kpi_id in (77, 78);
"""

query_update_manhour = """
UPDATE B
SET
    B.[kpi_id] = A.kpi_id,
    B.[yr] = A.[yr],
    B.[m01] = A.[m01],
    B.[m02] = A.[m02],
    B.[m03] = A.[m03],
    B.[m04] = A.[m04],
    B.[m05] = A.[m05],
    B.[m06] = A.[m06],
    B.[m07] = A.[m07],
    B.[m08] = A.[m08],
    B.[m09] = A.[m09],
    B.[m10] = A.[m10],
    B.[m11] = A.[m11],
    B.[m12] = A.[m12],
    B.[update_date] = GETDATE()
FROM [KPI_dtl] B
JOIN [dbo].[Manhour] A 
ON A.uniqueid = B.unique_id
WHERE B.kpi_id in (84, 85, 86, 87, 88, 89, 90);
"""

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
--WHERE yr = YEAR(GETDATE());

"""

def execute_query(query_name, query):
    try:
        with pyodbc.connect(constr) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
        print(f"Query '{query_name}' executed successfully.")
    except Exception as e:
        print(f"An error occurred while executing '{query_name}': {e}")

# Execute multiple queries with names
queries = [
    ("Backup Query", query_backup),
    ("Update Rework Query", query_update_rework),
    ("Update Deliver Query", query_update_deliver),
    ("Update Reject Query", query_update_reject),
    ("Update Manhour Query", query_update_manhour),
    ("Update Customer Feedback Request Query", query_update_CustFeedbackRequest),
    ("Update Total Average Query", query_update_total_average)
]

for query_name, query in queries:
    execute_query(query_name, query)