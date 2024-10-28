import pandas as pd
import os
from datetime import datetime

# Load the CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\72-73.csv')

# Get the current year
current_year = datetime.now().year

# ------------------------------------------------------------------------------ 
# Separate the month and year columns
df[['เดือน', 'ปี']] = df['เดือน'].str.extract(r'(.+?)\s(\d{4})')

# Rename columns to English
df.rename(columns={'เดือน': 'm', 'ปี': 'yr', 'หน่วยที่ใช้(ยูนิต)': '72', 'ค่าน้ำ(บาท)': '73'}, inplace=True)

# Strip whitespace from column names
df.columns = df.columns.str.strip()

# Check the columns and the first few rows of the DataFrame
# print(df.columns)  # Check the column names
# print(df.head())   # Check the first few rows to ensure correct loading

# Define a mapping for month names to codes
month_mapping = {
    'มกราคม': 'm01',
    'กุมภาพันธ์': 'm02',
    'มีนาคม': 'm03',
    'เมษายน': 'm04',
    'พฤษภาคม': 'm05',
    'มิถุนายน': 'm06',
    'กรกฎาคม': 'm07',
    'สิงหาคม': 'm08',
    'กันยายน': 'm09',
    'ตุลาคม': 'm10',
    'พฤศจิกายน': 'm11',
    'ธันวาคม': 'm12'
}

# Replace month names with codes using the mapping
df['m'] = df['m'].map(month_mapping)

# Print the DataFrame to verify month mapping
# print(df[['m', 'yr']])  # Check mapped month values and years

# Reorder columns
columns_order = ['yr', 'm', '72', '73']  # Use strings for column names
df = df[columns_order]

# Convert KWH and amount to numbers by removing commas and converting to float
df['72'] = df['72'].str.replace(',', '', regex=False).astype(float)
df['73'] = df['73'].str.replace(',', '', regex=False).astype(float)

# Convert year from Buddhist calendar (2567) to Gregorian calendar (2024)
df['yr'] = pd.to_numeric(df['yr'], errors='coerce')  # Convert to numeric, handle NaN
df = df[pd.notnull(df['yr'])]  # Remove rows where 'yr' is NaN or empty
df['yr'] = df['yr'].apply(lambda x: x - 543 if pd.notnull(x) else x)  # Convert year
df['yr'] = df['yr'].astype(int)  # Convert year to integer to remove .0

# ------------------------------------------------------------------------------
# Fill NaN values in the '66' and '67' columns with 0 without using inplace
df['72'] = df['72'].fillna(0)
df['73'] = df['73'].fillna(0)


# Pivot the data for '66'
df_72 = df.pivot(index='yr', columns='m', values='72')
df_73 = df.pivot(index='yr', columns='m', values='73')

df_72['kpi_id'] = 72
df_73['kpi_id'] = 73
# Combine the two DataFrames
df = pd.concat([df_72, df_73], ignore_index=True)

df['uniqueid'] = (str(current_year) + df['kpi_id'].astype(str)).astype(int)
df['yr'] = current_year

# ------------------------------------------------------------------------------ 
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path_combined = os.path.join(output_dir, '72-73.csv')  # Change filename if needed

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the reshaped DataFrame to CSV
df.to_csv(output_path_combined, index=False)

# Print the final reshaped DataFrame
print(df)