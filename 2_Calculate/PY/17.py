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