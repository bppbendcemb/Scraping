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
