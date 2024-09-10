import os
import requests
from bs4 import BeautifulSoup
import chardet
import csv

# ฟังก์ชันสำหรับบันทึกข้อมูลลงในไฟล์ CSV
def save_to_csv(headers, rows, file_name, folder_path):
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"ข้อมูลถูกบันทึกลงในไฟล์ {file_path}")

# URL ของเว็บที่คุณต้องการ scrape
url = 'http://bppnet/report/kpi/kpipdrw.aspx'

# ส่งคำขอ HTTP เพื่อดึงข้อมูลจากเว็บ
response = requests.get(url)

# ตรวจสอบ encoding ของเนื้อหาที่ดึงมา
result = chardet.detect(response.content)
encoding = result['encoding']
response.encoding = encoding

# ตรวจสอบ HTML ที่ได้
soup = BeautifulSoup(response.text, 'html.parser')

# สร้างโฟลเดอร์หากยังไม่สร้าง
folder_Output = 'Output'
if not os.path.exists(folder_Output):
    os.makedirs(folder_Output)

# หา table ที่มี id ต่างๆ
tables = {
    'MainContent_GridView1': 'KPI_Rework.csv',
    # 'MainContent_GridView3': 'KPI_pdrw.csv',
    'MainContent_GridView2': 'KPI_Lost.csv',
    'MainContent_GridView5': 'KPI_Repair_Color.csv'
}

# ตรวจสอบและบันทึกข้อมูลของแต่ละตาราง
for table_id, file_name in tables.items():
    table = soup.find(id=table_id)
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = [[td.text.strip() for td in row.find_all('td')] for row in table.find_all('tr')[1:]]
        save_to_csv(headers, rows, file_name, folder_Output)
    else:
        print(f"ไม่พบตารางที่มี id '{table_id}'")
