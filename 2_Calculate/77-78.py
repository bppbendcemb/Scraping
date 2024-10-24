import pandas as pd
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load CSV file
url = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\77-78.csv'
try:
    df = pd.read_csv(url)
except Exception as e:
    logging.error(f"Error reading CSV file: {e}")
    exit()

current_year = datetime.now().year

# Filter the DataFrame
df = df[df['ลำดับ'].isin([101, 102])]

# Rename columns
rename_dict = {
    'กิจกรรม': 'desc',
    'ม.ค.': 'm01',
    'ก.พ.': 'm02',
    'มี.ค.': 'm03',
    'เม.ย.': 'm04',
    'พ.ค.': 'm05',
    'มิ.ย.': 'm06',
    'ก.ค.': 'm07',
    'ส.ค.': 'm08',
    'ก.ย.': 'm09',
    'ต.ค.': 'm10',
    'พ.ย.': 'm11',
    'ธ.ค.': 'm12'
}

# Check if columns to rename exist
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)
    logging.info("Columns renamed successfully.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# Map KPI IDs and add year
maping = {101: '77', 102: '78'}
df['kpi_id'] = df['ลำดับ'].map(maping).fillna(-1).astype(int)
df['yr'] = current_year

# Create unique ID
df['uniqueid'] = (df['yr'].astype(str) + df['kpi_id'].astype(str)).astype(int)

# Reorder columns
df = df[['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']]

# Save DataFrame to CSV
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '77-78.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

try:
    df.to_csv(output_path, index=False)
    logging.info(f"Data saved to '{output_path}'.")
except Exception as e:
    logging.error(f"Error saving CSV file: {e}")

# Print the final DataFrame
print(df)
