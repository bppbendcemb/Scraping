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