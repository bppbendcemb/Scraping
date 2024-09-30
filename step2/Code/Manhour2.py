import os
import pandas as pd
import logging

# ตั้งค่าโฟลเดอร์ Input
folder_Input = 'step1\\Output'
input_file = os.path.join(folder_Input, 'Manhour1.csv')

# อ่านไฟล์ CSV
try:
    df = pd.read_csv(input_file)
except FileNotFoundError:
    logging.error(f"File '{input_file}' not found.")
    raise

# แสดงชื่อคอลัมน์
print("Columns in the DataFrame:", df.columns)

# ตรวจสอบคอลัมน์ 'เดือน'
if 'เดือน' in df.columns:
    # month_value = df['เดือน'].iloc[0]  # ใช้ค่าจากแถวแรกของเดือน
    # month_value = 'm' + df['เดือน'].iloc[0]  # ใช้ค่าจากแถวแรกของเดือน
    month_value = 'm' + str(df['เดือน'].iloc[0])  # Convert the month value to a string
else:
    logging.error("Column 'เดือน' not found in the DataFrame.")
    raise KeyError("Column 'เดือน' not found.")

# Dictionary สำหรับการเปลี่ยนชื่อคอลัมน์
rename_dict = {
    'เดือน': 'm' + str(month_value),  # ใช้ 'm' + month_value เพื่อให้เป็นชื่อที่ใช้ได้
    'ปี': 'yr',
    'ประเภท': 'catagory',
    'จำนวน(คน)': 'count_peple',
    'ชม.ทำงานปกติ': 'normalworkinghours',
}

# ตรวจสอบว่าคอลัมน์ที่ต้องการเปลี่ยนชื่อมีอยู่ใน DataFrame
if set(rename_dict.keys()).issubset(df.columns):
    df.rename(columns=rename_dict, inplace=True)

    # กำหนดชื่อไฟล์ใหม่ (หรือใช้ชื่อเดิม)
    output_file = os.path.join(folder_Input, 'Manhour2.csv')

    try:
        # บันทึกไฟล์ CSV ใหม่
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"Columns renamed and data saved to '{output_file}'.")
    except Exception as e:
        logging.error(f"Failed to save the file: {e}")
        raise
else:
    logging.warning("One or more columns to rename do not exist in the data.")

# สร้าง DataFrame ใหม่
data = {
    'yr': [df['yr'].iloc[0]] * 6,  # ใช้ค่าปีเดียวกับ df
    'activityid': [84, 85, 86, 87, 88, 89],
    'catagory': ['ปฏิบัติการ', 'ปฏิบัติการ', 'ปฏิบัติการ', 'สนับสนุน', 'สนับสนุน', 'สนับสนุน'],
    month_value: [
        df.iloc[0]['count_peple'], 
        df.iloc[0]['normalworkinghours'], 
        df.iloc[0]['OT'],
        df.iloc[1]['count_peple'], 
        df.iloc[1]['normalworkinghours'], 
        df.iloc[1]['OT']
    ]
}

# สร้าง DataFrame ใหม่จากข้อมูลที่เตรียมไว้
new_df = pd.DataFrame
