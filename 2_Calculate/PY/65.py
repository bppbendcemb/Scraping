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