import pandas as pd
import logging
import os  # For creating directories

# Set up logging level
logging.basicConfig(level=logging.WARNING)

# Input file path
Input_path = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\50_Deliver.csv'


# Read the CSV
df = pd.read_csv(Input_path)

# Clean the 'จำนวนชิ้นงาน' column (remove commas and convert to int)
df['จำนวนชิ้นงาน'] = df['จำนวนชิ้นงาน'].str.replace(',', '').astype(int)

# Rename columns
rename_dict = {
    'ปี': 'yr',
    'เดือน': 'm',
    'จำนวนชิ้นงาน': 'pieces'
}

# Check if all columns to rename exist
missing_columns = set(rename_dict.keys()) - set(df.columns)
if not missing_columns:
    df.rename(columns=rename_dict, inplace=True)
else:
    logging.warning(f"Columns missing: {missing_columns}")

# Extract the first year
yr = df['yr'].iloc[0]

# Pivot the data for 'pieces' (จำนวนชิ้นงาน)
pieces_df = df.pivot(index='yr', columns='m', values='pieces')

# Reindex to ensure all 12 months (1 to 12) are present, fill missing months with 'None'
months = list(range(1, 13))
pieces_df = pieces_df.reindex(columns=months, fill_value='None')

# Add metadata columns for output
kpi_id = 50  # Parameterized KPI ID
pieces_df.insert(0, 'uniqueid', str(yr) + str(kpi_id))
pieces_df.insert(1, 'yr', yr)
pieces_df.insert(2, 'kpi_id', kpi_id)

# Rename columns from numeric months to 'm01', 'm02', ..., 'm12'
pieces_df.columns = ['uniqueid', 'yr', 'kpi_id'] + [f'm{int(col):02}' for col in pieces_df.columns[3:]]


# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '50.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
pieces_df.to_csv(output_path, index=False)

# Print the final DataFrame
print(pieces_df)
