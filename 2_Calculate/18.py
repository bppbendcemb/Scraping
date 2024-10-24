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