import pandas as pd
from sqlalchemy import create_engine
import logging
import pyodbc
from datetime import datetime

# print(pyodbc.drivers())

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load data from CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\step2\Output\Rework_new.csv')

# Convert values in column m01 to float by removing commas
def convert_to_float(value):
    try:
        return float(str(value).replace(',', ''))
    except ValueError:
        return None

# Get m01 value for kpi_id = 99
try:
    m01_99 = df.loc[df['kpi_id'] == 99, 'm01'].values[0]
    m01_99 = convert_to_float(m01_99)
except IndexError:
    logging.error("Error: kpi_id = 99 not found in the dataset.")
    m01_99 = None

# Check that the value is not None and not 0 to avoid division by 0
if m01_99 and m01_99 != 0:
    # Calculate m01 to m12 values for all kpi_id except 99
    for month in ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']:
        df[month] = df.apply(
            lambda row: convert_to_float(row[month]) * 1000000 / m01_99 
            if row['kpi_id'] != 99 and pd.notnull(row[month]) 
            else convert_to_float(row[month]),  # Use original value for kpi_id = 99
            axis=1
        )

    # Add updatedate column with the current date
    df['updatedate'] = datetime.now()

    # SQL Server connection details
    server = 'c259-003\\SQLEXPRESS'
    database = 'KPI'
    #driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
    f"mssql+pyodbc://{server}/{database}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
)


    # Create engine for SQL Server connection
    engine = create_engine(connection_string)

    try:
        # Write DataFrame to SQL Server, creating a new table
        df.to_sql('Rework_new_calculated', con=engine, if_exists='replace', index=False)
        logging.info("DataFrame has been written to SQL Server successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
else:
    logging.warning("Cannot divide by 0 or insufficient data.")
