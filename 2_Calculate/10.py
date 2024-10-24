import pandas as  pd
import os  # For creating directories

# 10.Reject Calculate1

# Input file path
Input_path_10 = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\10.csv'
# Read the CSV

df_Reject = pd.read_csv(Input_path_10)

# Convert 'วันที่เอกสาร' column to datetime format in Reject DataFrame
df_Reject['วันที่เอกสาร'] = pd.to_datetime(df_Reject['วันที่เอกสาร'], format='%d/%m/%Y')

# Clean and convert 'Reject' column from text to numbers
df_Reject['Reject'] = df_Reject['Reject'].astype(str).str.replace(',', '').str.strip()
df_Reject['Reject'] = pd.to_numeric(df_Reject['Reject'], errors='coerce')

# เปลี่ยนชื่อหลายคอลัมน์ใน DataFrame
rename_columns = {
    'วันที่เอกสาร': 'date',  # เปลี่ยนชื่อเป็น 'date'
    'สถานะเอกสาร': 'status',  # เปลี่ยนชื่อเป็น 'status'
}

df_Reject.rename(columns=rename_columns, inplace=True)

# เพิ่มคอลัมน์ 'm' และ 'yr'
df_Reject['m'] = df_Reject['date'].dt.month
df_Reject['yr'] = df_Reject['date'].dt.year

# เลือกเฉพาะคอลัมน์ที่ต้องการใช้งาน
selected_columns = ['yr', 'm', 'status', 'Reject']
df_selected = df_Reject[selected_columns]

# ลบแถวที่ status = 'ยกเลิก'
df_filtered = df_selected[df_selected['status'] != 'ยกเลิก']

# เลือกเฉพาะคอลัมน์ 'yr', 'm', และ 'Reject'
selected_columns2 = ['yr', 'm', 'Reject']
df_selected2 = df_filtered[selected_columns2]

# Group by 'yr' และ 'm' และรวมค่าในคอลัมน์ 'Reject'
df_grouped = df_selected2.groupby(['yr', 'm'], as_index=False)['Reject'].sum()

# Pivot table: เปลี่ยนค่าของ 'm' เป็นคอลัมน์ และแสดงผลรวมของ 'Reject'
df_pivot = df_grouped.pivot_table(index='yr', columns='m', values='Reject', aggfunc='sum').reset_index()

# Create a DataFrame with all months (1-12)
all_months = pd.DataFrame(0, index=range(len(df_pivot)), columns=range(1, 13))  # Create columns for months 1-12
all_months['yr'] = df_pivot['yr']  # Add 'yr' column from df_pivot

# Fill in the values from df_pivot
for month in range(1, 13):
    if month in df_pivot.columns:
        all_months[month] = df_pivot[month].fillna(0)  # Fill NaN with 0

# Reorder columns to move 'yr' to the leftmost position
all_months = all_months[['yr'] + [col for col in all_months.columns if col != 'yr']]

# Rename columns
rename_dict = {
    1: 'm01',
    2: 'm02',
    3: 'm03',
    4: 'm04',
    5: 'm05',
    6: 'm06',
    7: 'm07',
    8: 'm08',
    9: 'm09',
    10: 'm10',
    11: 'm11',
    12: 'm12'
}

kpi_id = [10]
all_months.insert(loc=1, column='kpi_id', value=kpi_id)


# Check if columns to rename exist in all_months
if set(rename_dict.keys()).issubset(all_months.columns):
    all_months.rename(columns=rename_dict, inplace=True)
    
else:
    print("One or more columns to rename do not exist in all_months")

df_Reject = all_months
# df_Reject.to_csv('10XX.csv', index=False)

# --------------------------------------------------------------------------------------------------------------
# 10.Reject Calculate2

Input_path_50 = r'F:\_BPP\Project\Scraping\2_Calculate\CSV\50.csv'
df_deliver = pd.read_csv(Input_path_50)
# Union of the two DataFrames
df_union = pd.concat([df_Reject, df_deliver]).drop_duplicates().reset_index(drop=True)

# Filter for specific kpi_id
filtered_df = df_union[df_union['kpi_id'].isin([10, 50])]

# Get the values for kpi_id=50
kpi_50 = filtered_df[filtered_df['kpi_id'] == 50]

# Create a list to store results
results_list = []
yr_value = filtered_df['yr'].unique()[0]
# Calculate the ratio for each month
for month in range(1, 13):
    try:
        kpi_10_value = filtered_df[filtered_df['kpi_id'] == 10][f'm{month:02}'].values[0]  # Use m01, m02, ...
        kpi_50_value = kpi_50[f'm{month:02}'].values[0]  # Value for kpi_id=50 for that month

        # Calculate the ratio
        if kpi_50_value != 0:  # Check to avoid division by zero
            ratio = (kpi_10_value * 1000000) / kpi_50_value
        else:
            ratio = None  # If kpi_50_value is 0, set ratio to None

        # Add the data to the results list
        results_list.append({'yr': yr_value, 'kpi_id': 10, 'month': month, 'ratio': ratio})

    except IndexError:
        print(f"No data for month {month} for kpi_id 10 or 50.")
        continue

# Convert results list to DataFrame
result = pd.DataFrame(results_list)

# Add 'yr' and 'kpi_id' columns
result['yr'] = yr_value
result['kpi_id'] = 10
# Create 'uniqueid' column by concatenating 'yr' and 'kpi_id'
result['uniqueid'] = result['yr'].astype(str) + result['kpi_id'].astype(str)

# Reorder columns to place 'yr', 'kpi_id', and 'uniqueid' at the front
result = result[['uniqueid', 'yr', 'kpi_id', 'month', 'ratio']]

# Pivot the DataFrame to make it horizontal
result_pivot = result.pivot(index=['uniqueid','yr', 'kpi_id'], columns='month', values='ratio').reset_index()

# Rename columns for clarity
result_pivot.columns.name = None  # Remove the name of the columns
result_pivot.columns = [f'm{int(col):02}' if isinstance(col, int) else col for col in result_pivot.columns]

# Display the result DataFrame
print("Result DataFrame (Horizontal):")
print(result_pivot)

# Optional: Save the horizontal result DataFrame to CSV
df_Reject = result_pivot
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '10.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
df_Reject.to_csv(output_path, index=False)

# Print the final DataFrame
print(df_Reject)

#__________________________________________________________