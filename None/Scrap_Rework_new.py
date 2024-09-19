import csv
import os

# เปิดไฟล์ CSV และอ่านข้อมูล
input_file = r'f:\_BPP\Project\Scraping\Step1\Output\KPI_Rework.csv'  # ชื่อไฟล์ต้นฉบับ
output_file = 'Rework_new.csv'  # ชื่อไฟล์ผลลัพธ์

with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
    reader = csv.reader(csvfile)
    data = list(reader)

    # แมพชื่อเดือนเป็นรหัสที่สั้นลง
month_mapping = {
    'รายการ': 'activities',
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

# เปลี่ยนชื่อหัวตาราง
headers = ['uniqueid', 'yr', 'kpi_id'] + [month_mapping.get(header, header) for header in data[0][1:]]

# สร้างโฟลเดอร์หากยังไม่สร้าง
folder_Output = 'step2\Output'
if not os.path.exists(folder_Output):
    os.makedirs(folder_Output)

# วนลูปผ่านแต่ละแถวของข้อมูลและตรวจสอบเงื่อนไข
for row in data[1:]:
    # กำหนดค่า kpi_id ตามประเภท
    if row[1] == 'สาเหตุจากพนักงาน(ชิ้น)':
        kpi_id = '20'       
    elif row[1] == 'สาเหตุจากวัตถุดิบ(ชิ้น)':
        kpi_id = '21'
    elif row[1] == 'วัตถุดิบเฉพาะ Dis 442/Com054 (ชิ้น)':
        kpi_id = '22'
    elif row[1] == 'สาเหตุจากเครื่องจักร(ชิ้น)':
        kpi_id = '23'
    elif row[1] == 'สาเหตุจากวิธีการ/ควบคุม(ชิ้น)':
        kpi_id = '24'
    elif row[1] == 'สาเหตุจากแม่พิมพ์ (ชิ้น)':
        kpi_id = '25'
    elif row[1] == 'แม่พิมพ์เฉพาะ (Dis 177,258/Com054) (ชิ้น)':
        kpi_id = '26'
    elif row[1] == 'สาเหตุจากงานจ้างผลิตภายนอก (ชิ้น)':
        kpi_id = '27'
    elif row[1] == 'สาเหตุจากการออกแบบ (ชิ้น)':
        kpi_id = '28'
    elif row[1] == 'สาเหตุจากการซ่อมสี':
        kpi_id = '29'
    elif row[1] == 'สาเหตุจากคำสั่งพิเศษ':
        kpi_id = '30'
    elif row[1] == 'สาเหตุจากข้อร้องเรียนลูกค้า':
        kpi_id = '31'
    elif row[1] == 'ยอดผลิตรวม(ชิ้น)':
        kpi_id = '99'
    else:
        kpi_id = ''
    
    row.insert(1, kpi_id)  # ใส่ค่า kpi_id ลงในคอลัมน์ที่เพิ่มใหม่

    # สร้าง uniqueid โดยการรวมปีและ kpi_id
    year = row[0]
    uniqueid = f"{year}{kpi_id}"
    row.insert(0, uniqueid)  # ใส่ค่า uniqueid ลงในคอลัมน์ที่เพิ่มใหม่

# บันทึกข้อมูลใหม่ลงไฟล์ CSV ในโฟลเดอร์ Output
output_file_path = os.path.join(folder_Output, output_file)

with open(output_file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)  # เขียนหัวตาราง
    writer.writerows(data[1:])  # เขียนข้อมูล

print(f"บันทึกข้อมูลใหม่เรียบร้อยแล้วที่ {output_file_path}")
