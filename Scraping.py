import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import csv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

current_year = datetime.now().year
output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'

def fetch_table(url, table_id, output_file):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an error for bad responses
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': table_id})

        if table:
            headers = [header.text.strip() for header in table.find_all('th')]
            rows = []
            for row in table.find_all('tr')[1:]:
                columns = row.find_all('td')
                rows.append([column.text.strip() for column in columns])
            df = pd.DataFrame(rows, columns=headers)

            output_path = os.path.join(output_dir, output_file)
            os.makedirs(output_dir, exist_ok=True)
            df.to_csv(output_path, index=False)
            logging.info(f"Data saved to {output_file}")
        else:
            logging.warning("Table not found")
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Scraping logic
scraping_info = [
    {'url': 'http://bppnet/report/whiss.aspx', 'table_id': 'GridView1', 'output_file': '50_Deliver.csv'},
    {'url': f'http://bppnet/qm/report/ccrlst.aspx?ktype=yr&key={current_year}', 'table_id': 'ctl00_MainContent_datagrid1', 'output_file': '10.csv'},
    {'url': 'http://bppnet/report/kpi/kpipdrw.aspx', 'table_id': 'ctl00_MainContent_GridView1', 'output_file': '20-31.csv'},
    {'url': 'http://bppnet/report/kpi/kpipdrw.aspx', 'table_id': 'ctl00_MainContent_GridView2', 'output_file': '17.csv'},
    {'url': 'http://bppnet/report/kpi/kpipdrw.aspx', 'table_id': 'ctl00_MainContent_GridView5', 'output_file': '18.csv'},
    {'url': f'http://bppnet/report/pd/pdrwsumyr.aspx?yr{current_year}', 'table_id': 'ctl00_MainContent_GridView1', 'output_file': '65.csv'},
    {'url': 'http://bppnet/energy/report/energy.aspx', 'table_id': 'ctl00_MainContent_GridView1', 'output_file': '66-67.csv'},
    {'url': 'http://bppnet/energy/report/p2summary.aspx', 'table_id': 'ctl00_MainContent_GridView2', 'output_file': '68.csv'},
    {'url': 'http://bppnet/report/pd/pdpaint.aspx', 'table_id': 'ctl00_MainContent_GridView2', 'output_file': '69.csv'},
    {'url': 'http://bppnet/energy/report/energy.aspx', 'table_id': 'ctl00_MainContent_GridView2', 'output_file': '70.csv'},
    {'url': 'http://bppnet/energy/report/energy.aspx', 'table_id': 'ctl00_MainContent_GridView3', 'output_file': '72-73.csv'},
    {'url': 'http://bppnet/qm/report/ncstatus.aspx', 'table_id': 'ctl00_MainContent_GridView1', 'output_file': '77-78.csv'},
    # Add more URLs and table IDs here
]

for info in scraping_info:
    fetch_table(info['url'], info['table_id'], info['output_file'])
    time.sleep(2)  # Delay between requests

# ---------------------------------------------------------------------------------------------------------
# 84-90
# Enable logging
logging.basicConfig(level=logging.INFO)

# Step 1: Scrape data from 'http://bppnet/report/pd/bppmh.aspx' and save as CSV
# Define path to the WebDriver (e.g., ChromeDriver)
driver_path = 'C:/chromedriver/chromedriver.exe'
service = ChromeService(executable_path=driver_path)

# Create an instance of WebDriver
driver = webdriver.Chrome(service=service)

# Open the target webpage
url_8490 = 'http://bppnet/report/pd/bppmh.aspx'
driver.get(url_8490)

# Wait for the page to load (adjust if necessary)
time.sleep(2)

# Calculate the first day of the previous month
today = datetime.today()
first_day_of_current_month = today.replace(day=1)
first_day_of_last_month = first_day_of_current_month - timedelta(days=1)
first_day_of_last_month = first_day_of_last_month.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

# Show the results
# print("วันที่ 1 ของเดือนที่แล้ว:", first_day_of_last_month.strftime('%m/%d/%Y'))
# print("วันสุดท้ายของเดือนที่แล้ว:", last_day_of_last_month.strftime('%m/%d/%Y'))

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

    # print("ข้อมูลถูกบันทึกลงในไฟล์ : 84-90.csv")
    logging.info(f"Data saved to {file_path}")
else:
    print("ไม่พบตาราง")

# Close the browser when done
driver.quit()