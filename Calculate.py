import pandas as pd
import logging
import os  # For creating directories


# ---------------------------------------------------------------------------------------
# 50
# Set up logging level
logging.basicConfig(level=logging.WARNING)

# Input file path
Input_path = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\50.csv'


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

# ---------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------------------------------
# 17
import pandas as pd
import os
import logging
from datetime import datetime

df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\17.csv')
current_year = datetime.now().year
# -----------------------------------------------------------------------------------------------------------
# สร้าง column kpi_id
# Custom mapping of 'รายการ' to 'id'
item_id_mapping = {
    'การ Set Up เครื่อง/แม่พิมพ์ (MH)': 1,
    'จากเครื่องจักรเสีย (MH)': 2,
    'จากแม่พิมพ์เสีย (MH)': 3,
    'จากการรอวัสดุ/อุปกรณ์ (MH)': 4,
    'จากการไปผลิต/ทำงานอื่น (MH)': 5,
    'อื่น ๆ (MH) (ที่นอกเหนือหัวข้อข้างต้น)': 6,
    'MH ผลิตจริง (เฉพาะทีปิด)': 7,
    # 'เวลาสูญเสียจากการผลิต ต้องไม่เกิน': 17,
}

# Ensure 'รายการ' column exists before proceeding
if 'รายการ' in df.columns:
    # Map 'รายการ' to 'id' using the provided mapping
    df['kpi_id'] = df['รายการ'].map(item_id_mapping)

    # Convert 'id' column to int (in case there are NaN values, they will be replaced with -1 or some default value)
    df['kpi_id'] = df['kpi_id'].fillna(-1).astype(int)

    # logging.info("Mapped 'รายการ' to 'id'.")
else:
    logging.warning("Column 'รายการ' not found in the data.")

# -----------------------------------------------------------------------------------------------------------
# Rename columns เดือน
rename_dict = {
    'ปี': 'yr',
    'รายการ': 'desc',
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
    # df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    # logging.info(f"Columns renamed and data saved to '{updated_file_path}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# -----------------------------------------------------------------------------------------------------------
# เพิ่ม kpi_id_17 และ Sum kpi_id 2-6 ลงใน kpi_id_17
# ตรวจสอบว่ามี ID 17 อยู่ใน DataFrame หรือไม่
special_id = 17
if special_id not in df['kpi_id'].values:

     # ดึงปีจากคอลัมน์ 'ปี' (ตรวจสอบว่าคอลัมน์ 'ปี' มีอยู่ใน DataFrame)
    if 'ปี' in df.columns:
        year_value = df['ปี'].iloc[0]  # ดึงปีจากแถวแรก (คุณสามารถเลือกแถวที่ต้องการได้ตามต้องการ)
    else:
        logging.warning("Column 'ปี' not found in the data.")
        year_value = current_year  # ถ้าไม่พบปีให้ใช้ปีปัจจุบัน

    # เพิ่มแถวใหม่สำหรับ ID 17
    special_row = pd.DataFrame([{
        'yr': year_value,
        'desc': 'เวลาสูญเสียจากการผลิต ต้องไม่เกิน',
        'm01': 0, 'm02': 0, 'm03': 0, 'm04': 0, 'm05': 0, 'm06': 0,
        'm07': 0, 'm08': 0, 'm09': 0, 'm10': 0, 'm11': 0, 'm12': 0,
        'kpi_id': special_id
    }])
    df = pd.concat([df, special_row], ignore_index=True)

    # Sort DataFrame by 'id'
    df = df.sort_values(by='kpi_id')

    # df.to_csv(updated_file_path, index=False, encoding='utf-8-sig')
    # logging.info("เพิ่มแถวใหม่สำหรับ ID 17.")
else:
    logging.info("ID 17 มีอยู่ใน DataFrame แล้ว")


# Convert the 'm01' to 'm12' columns to numeric, coercing errors to NaN
sum_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
df[sum_columns] = df[sum_columns].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')

# Sum the values in 'm01' to 'm12' for rows with id 2 to 6
sum_values = df[df['kpi_id'].isin([2, 3, 4, 5, 6])][sum_columns].sum()

# Update only the row with id = 17
df.loc[df['kpi_id'] == 17, sum_columns] = sum_values.values

# -----------------------------------------------------------------------------------------------------------
# กรองแถวที่มี kpi_id เท่ากับ 7 และ 17
kpi_7 = df[df['kpi_id'] == 7]
kpi_17 = df[df['kpi_id'] == 17]

# ตรวจสอบว่ามีแถวที่เกี่ยวข้องทั้งสองหรือไม่
if not kpi_7.empty and not kpi_17.empty:
    # ทำการคำนวณเปอร์เซ็นต์ต่อเดือน (m01 ถึง m12) และบันทึกกลับไปที่แถว kpi_id = 17
    for month in ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']:
        df.loc[df['kpi_id'] == 17, month] = (kpi_17[month].values / kpi_7[month].values) * 100
    df = df[df['kpi_id'] == 17]
    # เพิ่มคอลัมน์ 'uniqueid' โดยคำนวณจาก 'yr' และ 'kpi_id'
    df['uniqueid'] = df['yr'].astype(str) + df['kpi_id'].astype(str)
    
    # จัดเรียงคอลัมน์ตามลำดับที่กำหนด
    df = df[['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']]

    # แสดงผลลัพธ์หลังจากบันทึกเสร็จ
    # print(df[df['kpi_id'] == 17])
else:
    print("ไม่พบข้อมูลสำหรับ kpi_id 7 หรือ kpi_id 17")
# -----------------------------------------------------------------------------------------------------------
# บันทึกไฟล์ CSV
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '17.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
# df.to_csv(output_path, index=False)
df.to_csv(output_path, index=False)
# Print the final DataFrame
print(df)

# -----------------------------------------------------------------------------------------------------------
# 18
import pandas as pd
import os
import logging
from datetime import datetime

url = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\18.csv'
df = pd.read_csv(url)
current_year = datetime.now().year
# -----------------------------------------------------------------------------------------------------------
# Rename columns เดือน
rename_dict = {
    'ปี': 'yr',
    'รายการ': 'desc',
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
    logging.info(f"Columns renamed and data saved to '{df}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")
# -----------------------------------------------------------------------------------------------------------
maping = {'% ซ่อมสี:':18}
df['kpi_id'] = df['desc'].map(maping)
df['kpi_id'] = df['kpi_id'].fillna(-1).astype(int)

# ดึงปีจากคอลัมน์ 'ปี' (ตรวจสอบว่าคอลัมน์ 'ปี' มีอยู่ใน DataFrame)
if 'ปี' in df.columns:
    year_value = df['yr'].iloc[0]  # ดึงปีจากแถวแรก (คุณสามารถเลือกแถวที่ต้องการได้ตามต้องการ)
else:
    logging.warning("Column 'ปี' not found in the data.")
    year_value = current_year  # ถ้าไม่พบปีให้ใช้ปีปัจจุบัน
df['yr'] = year_value

df = df[df['kpi_id'] == 18]
# เพิ่มคอลัมน์ 'uniqueid' โดยคำนวณจาก 'yr' และ 'kpi_id'
df['uniqueid'] = df['yr'].astype(str) + df['kpi_id'].astype(str)
    
# จัดเรียงคอลัมน์ตามลำดับที่กำหนด
df = df[['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']]

# -----------------------------------------------------------------------------------------------------------
# บันทึกไฟล์ CSV
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '18.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
df.to_csv(output_path, index=False)
# Print the final DataFrame
print(df)

# -----------------------------------------------------------------------------------------------------------
# 20-31
import pandas as pd
import os
import logging
from datetime import datetime

url = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\20-31.csv'
df = pd.read_csv(url)
current_year = datetime.now().year
# -----------------------------------------------------------------------------------------------------------
# Rename columns เดือน
rename_dict = {
    'ปี': 'yr',
    'รายการ': 'desc',
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
    logging.info(f"Columns renamed and data saved to '{df}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")
# -----------------------------------------------------------------------------------------------------------
maping = {
    'สาเหตุจากพนักงาน(ชิ้น)': 20,
    'สาเหตุจากวัตถุดิบ(ชิ้น)': 21,
    'วัตถุดิบเฉพาะ Dis 442/Com054 (ชิ้น)': 22,
    'สาเหตุจากเครื่องจักร(ชิ้น)': 23,
    'สาเหตุจากวิธีการ/ควบคุม(ชิ้น)': 24,
    'สาเหตุจากแม่พิมพ์ (ชิ้น)': 25,
    'แม่พิมพ์เฉพาะ (Dis 177,258/Com054) (ชิ้น)': 26,
    'สาเหตุจากงานจ้างผลิตภายนอก (ชิ้น)': 27,
    'สาเหตุจากการออกแบบ (ชิ้น)': 28,
    'สาเหตุจากการซ่อมสี': 29,
    'สาเหตุจากคำสั่งพิเศษ': 30,
    'สาเหตุจากข้อร้องเรียนลูกค้า': 31,
    'ยอดผลิตรวม(ชิ้น)': 99,
          }
df['kpi_id'] = df['desc'].map(maping)
df['kpi_id'] = df['kpi_id'].fillna(-1).astype(int)

# ดึงปีจากคอลัมน์ 'ปี' (ตรวจสอบว่าคอลัมน์ 'ปี' มีอยู่ใน DataFrame)
# if 'ปี' in df.columns:
#     year_value = df['yr'].iloc[0]  # ดึงปีจากแถวแรก (คุณสามารถเลือกแถวที่ต้องการได้ตามต้องการ)
# else:
#     logging.warning("Column 'ปี' not found in the data.")
#     year_value = current_year  # ถ้าไม่พบปีให้ใช้ปีปัจจุบัน
# df['yr'] = year_value

# df = df[df['kpi_id'] == 18]
# เพิ่มคอลัมน์ 'uniqueid' โดยคำนวณจาก 'yr' และ 'kpi_id'
df['uniqueid'] = df['yr'].astype(str) + df['kpi_id'].astype(str)
    
# จัดเรียงคอลัมน์ตามลำดับที่กำหนด
df = df[['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']]
# Sort DataFrame by 'id'
df = df.sort_values(by='kpi_id')

# ----------------------------------------------------------------------------------------------------------
# List of ids to process
ids = [20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]

# Define the month columns
months = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']

# Check if the row with kpi_id 99 exists and is not empty
if not df[df['kpi_id'] == 99].empty:
    row_99 = df[df['kpi_id'] == 99].iloc[0] 
    # Remove commas and convert to float, replacing 0 with NaN
    row_99_values = row_99[months].astype(str).str.replace(',', '').astype(float).replace(0, float('nan'))

    for id_val in ids:
        if not df[df['kpi_id'] == id_val].empty:
            row = df[df['kpi_id'] == id_val].iloc[0]

            # Perform the calculation, ensuring we handle strings in the same way
            result = (row[months].astype(str).str.replace(',', '').astype(float) / row_99_values) * 1000000
            
            # Update the values in the DataFrame
            df.loc[df['kpi_id'] == id_val, months] = result.values
            
            print(f"Row with id={id_val} updated successfully!")
        else:
            print(f"Row with id={id_val} does not exist in the DataFrame.")
    
    print("All updates completed successfully!")
else:
    print("Row with id=99 is missing or empty.")

# Remove rows with kpi_id 99
df = df[df['kpi_id'] != 99]

# -----------------------------------------------------------------------------------------------------------
# บันทึกไฟล์ CSV
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '20-31.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
df.to_csv(output_path, index=False)
# Print the final DataFrame
print(df)

# -----------------------------------------------------------------------------------------------------------
# 65
import pandas as pd
import os
import logging
from datetime import datetime

url = r'F:\_BPP\Project\Scraping\1_Scraping\CSV\65.csv'
df = pd.read_csv(url)
current_year = datetime.now().year
# -----------------------------------------------------------------------------------------------------------
"""
สาเหตุ,รวม (บาท),ม.ค.,ก.พ.,มี.ค.,เม.ย.,พ.ค.,มิ.ย.,ก.ค.,ส.ค.,ก.ย.,ต.ค.,พ.ย.,ธ.ค.
ซ่อมสี,"45,643","2,355","1,192","27,430","2,116","2,572","5,469","1,344","2,045","1,120",0,0,0
วัตถุดิบ,"39,970","2,128","9,380","4,228","3,892",0,"7,980","1,820","1,582","8,960",0,0,0
แม่พิมพ์,"38,633",0,840,0,0,"5,740","3,780","9,660","11,557","4,956","2,100",0,0
อื่นๆ,"17,147",0,0,0,"10,360","1,120","3,287",0,0,0,"2,380",0,0
วิธีการ,"9,380","1,820","2,660",0,"1,680",0,"3,080",140,0,0,0,0,0
ผู้ส่งมอบ,"3,920",0,0,"3,920",0,0,0,0,0,0,0,0,0
พนักงาน,"3,080",140,700,0,0,"1,120",0,0,"1,120",0,0,0,0
รวม:,"157,773","6,443","14,772","35,578","18,048","10,552","23,596","12,964","16,304","15,036","4,480",0,0
"""

# Rename columns เดือน
rename_dict = {
    'สาเหตุ': 'cause',
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
    logging.info(f"Columns renamed and data saved to '{df}'.")
else:
    logging.warning("One or more columns to rename do not exist in the data.")
# -----------------------------------------------------------------------------------------------------------
maping = {'รวม:':65}
df['kpi_id'] = df['cause'].map(maping)
df['kpi_id'] = df['kpi_id'].fillna(-1).astype(int)
df = df[df['kpi_id'] == 65]

df['yr'] = current_year

# เพิ่มคอลัมน์ 'uniqueid' โดยคำนวณจาก 'yr' และ 'kpi_id'
df['uniqueid'] = df['yr'].astype(str) + df['kpi_id'].astype(str)
    
# จัดเรียงคอลัมน์ตามลำดับที่กำหนด
df = df[['uniqueid', 'yr', 'kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']]

# -----------------------------------------------------------------------------------------------------------
# ลบเครื่องหมายจุลภาคออกจากคอลัมน์ตัวเลข
for column in ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']:
    df[column] = df[column].replace(',', '', regex=True).astype(float)  # ลบจุลภาคและแปลงเป็น float


# บันทึกไฟล์ CSV
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '65.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
df.to_csv(output_path, index=False)
# Print the final DataFrame
print(df)

# -----------------------------------------------------------------------------------------------------------
# 66-67

import pandas as pd
import os
from datetime import datetime

# Load the CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\66-67.csv')

# Get the current year
current_year = datetime.now().year

# ------------------------------------------------------------------------------ 
# Separate the month and year columns
df[['เดือน', 'ปี']] = df['เดือน'].str.extract(r'(.+?)\s(\d{4})')

# Rename columns to English
df.rename(columns={'เดือน': 'm', 'ปี': 'yr', 'KWH': '66', 'จำนวนเงิน': '67'}, inplace=True)

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
columns_order = ['yr', 'm', '66', '67']  # Use strings for column names
df = df[columns_order]

# Convert KWH and amount to numbers by removing commas and converting to float
df['66'] = df['66'].str.replace(',', '', regex=False).astype(float)
df['67'] = df['67'].str.replace(',', '', regex=False).astype(float)

# Convert year from Buddhist calendar (2567) to Gregorian calendar (2024)
df['yr'] = pd.to_numeric(df['yr'], errors='coerce')  # Convert to numeric, handle NaN
df = df[pd.notnull(df['yr'])]  # Remove rows where 'yr' is NaN or empty
df['yr'] = df['yr'].apply(lambda x: x - 543 if pd.notnull(x) else x)  # Convert year
df['yr'] = df['yr'].astype(int)  # Convert year to integer to remove .0

# ------------------------------------------------------------------------------
# Fill NaN values in the '66' and '67' columns with 0 without using inplace
df['66'] = df['66'].fillna(0)
df['67'] = df['67'].fillna(0)


# Pivot the data for '66'
df_66 = df.pivot(index='yr', columns='m', values='66')
df_67 = df.pivot(index='yr', columns='m', values='67')

df_66['kpi_id'] = 66
df_67['kpi_id'] = 67
# Combine the two DataFrames
df = pd.concat([df_66, df_67], ignore_index=True)

df['uniqueid'] = (str(current_year) + df['kpi_id'].astype(str)).astype(int)
df['yr'] = current_year

# ------------------------------------------------------------------------------ 
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path_combined = os.path.join(output_dir, '66-67.csv')  # Change filename if needed

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the reshaped DataFrame to CSV
df.to_csv(output_path_combined, index=False)

# Print the final reshaped DataFrame
print(df)

# ------------------------------------------------------------------------------ 
# 68
import pandas as pd
import os
from datetime import datetime

# Load the CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\68.csv')

# Get the current year
current_year = datetime.now().year

# ------------------------------------------------------------------------------ 
# Separate the month and year columns
df[['เดือน', 'ปี']] = df['เดือน'].str.extract(r'(.+?)\s(\d{4})')

# Rename columns to English
df.rename(columns={'เดือน': 'm', 'ปี': 'yr', 'ยอดรวมการใช้ LPG(กก.)': '68'}, inplace=True)

# Strip whitespace from column names
df.columns = df.columns.str.strip()

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

# Reorder columns
columns_order = ['yr', 'm', '68']  # Use strings for column names
df = df[columns_order]

# Convert KWH and amount to numbers by removing commas and converting to float
df['68'] = df['68'].str.replace(',', '', regex=False).astype(float)


# Convert year from Buddhist calendar (2567) to Gregorian calendar (2024)
df['yr'] = pd.to_numeric(df['yr'], errors='coerce')  # Convert to numeric, handle NaN
df = df[pd.notnull(df['yr'])]  # Remove rows where 'yr' is NaN or empty
df['yr'] = df['yr'].apply(lambda x: x - 543 if pd.notnull(x) else x)  # Convert year
df['yr'] = df['yr'].astype(int)  # Convert year to integer to remove .0

# ------------------------------------------------------------------------------
# Fill NaN values in the '66' and '67' columns with 0 without using inplace
df['68'] = df['68'].fillna(0)

# Pivot the data for '66'
df = df.pivot(index='yr', columns='m', values='68')


df['kpi_id'] = 68


df['uniqueid'] = (str(current_year) + df['kpi_id'].astype(str)).astype(int)
df['yr'] = current_year

# ------------------------------------------------------------------------------ 
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path_combined = os.path.join(output_dir, '68.csv')  # Change filename if needed

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the reshaped DataFrame to CSV
df.to_csv(output_path_combined, index=False)

# Print the final reshaped DataFrame
print(df)

# ----------------------------------------------------------------------------------
# 69

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

# ------------------------------------------------------------------------------
# 70
import pandas as pd
import os
from datetime import datetime

# Load the CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\70.csv')

# Get the current year
current_year = datetime.now().year

# ------------------------------------------------------------------------------ 
# Separate the month and year columns
df[['เดือน', 'ปี']] = df['เดือน'].str.extract(r'(.+?)\s(\d{4})')

# Rename columns to English
df.rename(columns={'เดือน': 'm', 'ปี': 'yr', 'น้ำหนักชิ้นงานพ่นสี(ตัน)': '70'}, inplace=True)

# Strip whitespace from column names
df.columns = df.columns.str.strip()

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

# Reorder columns
columns_order = ['yr', 'm', '70']  # Use strings for column names
df = df[columns_order]

# Ensure '70' is treated as a string for replacement and handle empty strings
df['70'] = df['70'].fillna('').astype(str)  # Fill NaN with empty string and convert to string
df['70'] = df['70'].str.replace(',', '', regex=False).replace('', '0')  # Replace empty strings with '0'
df['70'] = pd.to_numeric(df['70'], errors='coerce')  # Convert to numeric, NaNs will be created from invalid parsing

# Convert year from Buddhist calendar (2567) to Gregorian calendar (2024)
df['yr'] = pd.to_numeric(df['yr'], errors='coerce')  # Convert to numeric, handle NaN
df = df[pd.notnull(df['yr'])]  # Remove rows where 'yr' is NaN or empty
df['yr'] = df['yr'].apply(lambda x: x - 543 if pd.notnull(x) else x)  # Convert year
df['yr'] = df['yr'].astype(int)  # Convert year to integer to remove .0

# ------------------------------------------------------------------------------ 
# Fill NaN values in the '70' column with 0 without using inplace
df['70'] = df['70'].fillna(0)

# Pivot the data for '70'
df = df.pivot(index='yr', columns='m', values='70')

# Add additional columns
df['kpi_id'] = 70
df['uniqueid'] = (str(current_year) + df['kpi_id'].astype(str)).astype(int)
df['yr'] = current_year

# ------------------------------------------------------------------------------ 
# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path_combined = os.path.join(output_dir, '70.csv')  # Change filename if needed

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the reshaped DataFrame to CSV
df.to_csv(output_path_combined, index=False)

# Print the final reshaped DataFrame
print(df)

# ------------------------------------------------------------------------------ 
# 72-73

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

# ------------------------------------------------------------------------------ 
# 77-78

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

# ------------------------------------------------------------------------------ 
# 84-90
import os
import pandas as pd
import logging

# แปลงข้อมูลไปตามหัวข้อ 84 - 90 แล้วบันทึกไฟล์ csv
# Load CSV file
df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\84-90.csv')

# ตรวจสอบคอลัมน์ 'เดือน'
if 'เดือน' in df.columns:
    df['เดือน'] = df['เดือน'].apply(lambda x: f'{int(x):02}')
    month_value = str(df['เดือน'].iloc[0])  # ใช้ค่าจากแถวแรกของเดือน
    
else:
    logging.error("Column 'เดือน' not found in the DataFrame.")
    raise KeyError("Column 'เดือน' not found.")

# Dictionary สำหรับการเปลี่ยนชื่อคอลัมน์
rename_dict = {
    'เดือน': month_value,  # ใช้ 'm' + month_value เพื่อให้เป็นชื่อที่ใช้ได้
    'ปี': 'yr',
    'ประเภท': 'catagory',
    'จำนวน(คน)': 'count_peple',
    'ชม.ทำงานปกติ': 'normalworkinghours',
}

# ตรวจสอบว่าคอลัมน์ที่ต้องการเปลี่ยนชื่อมีอยู่ใน DataFrame
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# Remove commas and convert the columns to numeric
df['normalworkinghours'] = pd.to_numeric(df['normalworkinghours'].str.replace(',', ''), errors='coerce')
df['OT'] = pd.to_numeric(df['OT'].str.replace(',', ''), errors='coerce')

# สร้าง DataFrame ใหม่
data = {
    'yr': [df['yr'].iloc[0]] * 7,  # ใช้ค่าปีเดียวกับ df
    'm': month_value,
    'kpi_id': [84, 85, 86, 87, 88, 89, 90],
    'catagory': ['ปฏิบัติการ', 'ปฏิบัติการ', 'ปฏิบัติการ', 'สนับสนุน', 'สนับสนุน', 'สนับสนุน', 'รวม'],   
    'm' + month_value : [
        df.iloc[0]['count_peple'], 
        df.iloc[0]['normalworkinghours'], 
        df.iloc[0]['OT'],
        df.iloc[1]['count_peple'], 
        df.iloc[1]['normalworkinghours'], 
        df.iloc[1]['OT'],
        df.iloc[0]['normalworkinghours'] + df.iloc[0]['OT'] + df.iloc[1]['normalworkinghours'] + df.iloc[1]['OT'], #ผลรวมของ 85,86,88,89
    ]
}

# สร้าง DataFrame ใหม่จากข้อมูลที่เตรียมไว้
new_df = pd.DataFrame(data)

# Create the 'uniqueid' by combining 'yr' and 'activityid'
new_df['uniqueid'] = new_df['yr'].astype(str) + new_df['kpi_id'].astype(str)
df = new_df
# --------------------------------------------------------------------------------------------
"""
yr,m,kpi_id,catagory,m09
2024,09,84,ปฏิบัติการ,111.0
2024,09,85,ปฏิบัติการ,19316.0
2024,09,86,ปฏิบัติการ,3014.0
2024,09,87,สนับสนุน,80.0
2024,09,88,สนับสนุน,11776.0
2024,09,89,สนับสนุน,490.5
2024,09,90,รวม,34596.5
"""
# Reorder columns

# --------------------------------------------------------------------------------------------
# Save DataFrame to CSV
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '84-90.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

try:
    df.to_csv(output_path, index=False)
    logging.info(f"Data saved to '{output_path}'.")
except Exception as e:
    logging.error(f"Error saving CSV file: {e}")

# Print the final DataFrame
print(df)

