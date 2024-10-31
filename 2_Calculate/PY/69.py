import pandas as pd
import os
from datetime import datetime
import numpy as np  # นำเข้า numpy สำหรับใช้ NaN

# Load the CSV file
try:
    df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\69.csv')
except Exception as e:
    print(f"Error loading CSV file: {e}")
    exit()

# Get the current year
current_year = datetime.now().year

# Rename columns to English
try:
    df.rename(columns={'เดือน': 'm', 'ปี': 'yr', 'น้ำหนักรวม(กก.)': '69'}, inplace=True)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Define a mapping for month numbers to codes
    month_mapping = {
        1: 'm01', 2: 'm02', 3: 'm03', 4: 'm04',
        5: 'm05', 6: 'm06', 7: 'm07', 8: 'm08',
        9: 'm09', 10: 'm10', 11: 'm11', 12: 'm12'
    }

    # Replace month numbers with codes using the mapping
    df['m'] = df['m'].map(month_mapping)

    # Convert 'น้ำหนักรวม(กก.)' (now '69') to numbers by removing commas
    df['69'] = df['69'].str.replace(',', '', regex=False).astype(float)

    # Convert Buddhist year to Gregorian calendar
    # df['yr'] = pd.to_numeric(df['yr'], errors='coerce')
    # df = df[pd.notnull(df['yr'])]  # Remove rows where 'yr' is NaN
    # df['yr'] = df['yr'].apply(lambda x: x - 543 if pd.notnull(x) else x).astype(int)

    # ------------------------------------------------------------------------------ 
    # Pivot the data for '69' to create wide format
    df_pivot = df.pivot_table(index='yr', columns='m', values='69', aggfunc='sum')

    # Ensure all month columns exist, filling with NaN if they don't
    for month in ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']:
        if month not in df_pivot.columns:
            df_pivot[month] = np.nan

    # Fill NaN values with empty strings for the pivoted columns
    df_pivot = df_pivot.fillna('')

    # Reset the index to make 'yr' a column
    df_pivot.reset_index(inplace=True)

    # Add additional columns for 'kpi_id' and 'uniqueid'
    df_pivot['kpi_id'] = 69
    df_pivot['uniqueid'] = (str(current_year) + df_pivot['kpi_id'].astype(str)).astype(int)
    
    # Reorder columns to put 'yr' first and 'kpi_id' and 'uniqueid' last
    columns_order = ['yr', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12', 'kpi_id', 'uniqueid']
    df_pivot = df_pivot[columns_order]

    # Output file path
    output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
    output_path = os.path.join(output_dir, '69.csv')

    # Create the directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Save the reshaped DataFrame to CSV
    df_pivot.to_csv(output_path, index=False)

    # Print the final reshaped DataFrame
    print(df_pivot)

except Exception as e:
    print(f"An error occurred during processing: {e}")
