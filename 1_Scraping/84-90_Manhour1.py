from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import csv
import os
import logging

# Enable logging
logging.basicConfig(level=logging.INFO)

# Step 1: Scrape data from 'http://bppnet/report/pd/bppmh.aspx' and save as CSV
# Define path to the WebDriver (e.g., ChromeDriver)
driver_path = 'C:/chromedriver/chromedriver.exe'
service = ChromeService(executable_path=driver_path)

# Create an instance of WebDriver
driver = webdriver.Chrome(service=service)

# Open the target webpage
url = 'http://bppnet/report/pd/bppmh.aspx'
driver.get(url)

# Wait for the page to load (adjust if necessary)
time.sleep(2)

# Calculate the first day of the previous month
today = datetime.today()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

# Show the results
print("วันที่ 1 ของเดือนที่แล้ว:", first_day_of_last_month.strftime('%m/%d/%Y'))
print("วันสุดท้ายของเดือนที่แล้ว:", last_day_of_last_month.strftime('%m/%d/%Y'))

# Use JavaScript to set input values
script = f"""
    document.getElementById('ctl00_MainContent_TextYR').value = '{first_day_of_last_month.strftime('%m/%d/%Y')}'; 
    document.getElementById('ctl00_MainContent_TextYR0').value = '{last_day_of_last_month.strftime('%m/%d/%Y')}';
"""
driver.execute_script(script)

# Find the "ดูข้อมูล" button and click it
try:
    submit_button = driver.find_element(By.ID, 'ctl00_MainContent_Button1')
    submit_button.click()
except Exception as e:
    logging.error("Could not find the submit button: %s", e)
    driver.quit()
    exit()

# Wait for the page to load after clicking the button
time.sleep(5)  # Adjust time if necessary

# Get the HTML after the button is clicked
page_html = driver.page_source

# Use BeautifulSoup to scrape the data
soup = BeautifulSoup(page_html, 'html.parser')

# Get the table data
table = soup.find('table', {'id': 'ctl00_MainContent_GridView1'})  # Adjust as needed
if table:
    # Get headers
    headers = [header.text.strip() for header in table.find_all('th')]
    
    # Insert month and year into headers
    headers.insert(0, 'เดือน')
    headers.insert(1, 'ปี')

    # Get rows
    rows = table.find_all('tr')

    # Calculate month and year
    month = first_day_of_last_month.strftime('%m')
    year = first_day_of_last_month.strftime('%Y')
    
    # กำหนดที่อยู่สำหรับบันทึกไฟล์ CSV
    output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
    
    # ตรวจสอบว่ามีโฟลเดอร์อยู่หรือไม่ ถ้าไม่มีก็สร้างขึ้นมา
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # กำหนด path สำหรับไฟล์ CSV
    file_path = os.path.join(output_dir, '84-90.csv')
    
    # Save data to CSV
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
    
            # Write rows (skip the header row)
            for row in rows[1:]:  # Start from the second row to skip headers
                cols = row.find_all('td')
                if cols:
                    row_data = [month, year] + [col.text.strip() for col in cols]
                    writer.writerow(row_data)

    print("ข้อมูลถูกบันทึกลงในไฟล์ : 84-90.csv")

else:
    print("ไม่พบตาราง")

# Close the browser when done
driver.quit()
