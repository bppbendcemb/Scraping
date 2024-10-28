from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import csv
import os

# step1 Scraping ข้อมูลจาก 'http://bppnet/report/pd/bppmh.aspx' แล้วบันทึกเป็น csv


# กำหนด path ไปที่ WebDriver ของเบราว์เซอร์ (เช่น ChromeDriver)
driver_path = 'C:/chromedriver/chromedriver.exe'
service = ChromeService(executable_path=driver_path)

# สร้าง instance ของ WebDriver
driver = webdriver.Chrome(service=service)

# เปิดหน้าเว็บที่ต้องการ
url = 'http://bppnet/report/pd/bppmh.aspx'
driver.get(url)

# รอให้หน้าเว็บโหลด (ถ้ามีการโหลดข้อมูลหรือ JavaScript)
time.sleep(2)

# คำนวณวันที่ 1 ของเดือนก่อนหน้า
today = datetime.today()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

# แสดงผลลัพธ์
print("วันที่ 1 ของเดือนที่แล้ว:", first_day_of_last_month.strftime('%m/%d/%Y'))
print("วันสุดท้ายของเดือนที่แล้ว:", last_day_of_last_month.strftime('%m/%d/%Y'))

# ใช้ JavaScript เปลี่ยนค่าใน <input>
# ctl00_MainContent_TextYR
script = f"""
    document.getElementById('ctl00_MainContent_TextYR').value = '{first_day_of_last_month.strftime('%m/%d/%Y')}'; 
    document.getElementById('ctl00_MainContent_TextYR0').value = '{last_day_of_last_month.strftime('%m/%d/%Y')}';
"""
driver.execute_script(script)

# รอให้หน้าเว็บโหลดข้อมูลใหม่หลังจากเปลี่ยนค่า
# time.sleep(5)  
# ปรับเวลาให้เหมาะสม

# ค้นหาปุ่ม "ดูข้อมูล" และคลิกมัน
submit_button = driver.find_element(By.ID, 'ctl00_MainContent_Button1')
submit_button.click()

# รอให้หน้าเว็บโหลดข้อมูลใหม่หลังจากการคลิกปุ่ม
# time.sleep(10)  
# ปรับเวลาให้เหมาะสม

# ดึง HTML หลังจากที่ปุ่มถูกคลิก
page_html = driver.page_source

# ใช้ BeautifulSoup เพื่อ scraping ข้อมูล
soup = BeautifulSoup(page_html, 'html.parser')

# ดึงข้อมูลจาก table
table = soup.find('table', {'id': 'ctl00_MainContent_GridView1'})  # ปรับให้ตรงกับโครงสร้างเว็บ
if table:
    # ดึง headers
    headers = [header.text.strip() for header in table.find_all('th')]
    
     # แทรกคอลัมน์เดือนและปีใน headers
    headers.insert(0, 'เดือน')
    headers.insert(1, 'ปี')

    # ดึง rows
    rows = table.find_all('tr')

      # คำนวณเดือนและปี
    month = first_day_of_last_month.strftime('%m')
    year = first_day_of_last_month.strftime('%Y')
    
    folder_Output = 'step1\Output'
    if not os.path.exists(folder_Output):
        os.makedirs(folder_Output)

    file_path = os.path.join(folder_Output, 'Manhour1.csv')

    # บันทึกข้อมูลลงในไฟล์ CSV
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        # เขียน rows (เริ่มจากแถวที่สองเพื่อข้าม headers)
        for row in rows:
            cols = row.find_all('td')
            if cols:
                row_data = [month, year] + [col.text.strip() for col in cols]
                writer.writerow(row_data)
    
    print("ข้อมูลถูกบันทึกลงในไฟล์ : Manhour1.csv")
else:
    print("ไม่พบตาราง")

# ปิดเบราว์เซอร์เมื่อทำเสร็จ
driver.quit()

#-------------------------------------------------------
# เดือน,ปี,ประเภท,จำนวน(คน),ชม.ทำงานปกติ,OT
# 08,2024,ปฏิบัติการ,112,"18,854.00","1,210.00"
# 08,2024,สนับสนุน,80,"11,408.00",364.50

input_file = os.path.join(folder_Output, 'Manhour1.csv')

