import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import pandas as pd
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
import time
import logging

current_year = datetime.now().year
output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
# ------------------------------------------------------------------------------------------------
# 50 Deliver
url_50 = 'http://bppnet/report/whiss.aspx'
response = requests.get(url_50)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_deliver = pd.DataFrame(rows, columns=headers)   

        # Output file path
    
        output_path = os.path.join(output_dir, '50_Deliver.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_deliver.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 50_Deliver.csv")    

    else:
        print("No Table")
else:
    print(f"Page Not Found: {response.status_code}")

# ------------------------------------------------------------------------------------------------
# 10.Reject
url_10 = f'http://bppnet/qm/report/ccrlst.aspx?ktype=yr&key={current_year}'
response = requests.get(url_10)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_datagrid1'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_Reject = pd.DataFrame(rows, columns=headers) 

        # Output file path
       
        output_path = os.path.join(output_dir, '10.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_Reject.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 10.csv")         
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ------------------------------------------------------------------------------------------------
url_1731 = 'http://bppnet/report/kpi/kpipdrw.aspx'

response = requests.get(url_1731)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table1 = soup.find('table',{'id':'ctl00_MainContent_GridView1'})  # ctl00_MainContent_datagrid1
    table2 = soup.find('table',{'id':'ctl00_MainContent_GridView2'})  # ctl00_MainContent_datagrid1
    table5 = soup.find('table',{'id':'ctl00_MainContent_GridView5'})  # ctl00_MainContent_datagrid1

# ---------------------------------------------------------------------------------------------------------
# 20-31 
#   
    if table1:
        headers = [header.text.strip() for header in table1.find_all('th')]
        rows = []
        for row in table1.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_ReworkLost1 = pd.DataFrame(rows, columns=headers)

        # Output file path
       
        output_path = os.path.join(output_dir, '20-31.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_ReworkLost1.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 20-31.csv")         
    else:
        print("No This Table")
# ---------------------------------------------------------------------------------------------------------
# 17

    if table2:
        headers = [header.text.strip() for header in table2.find_all('th')]
        rows = []
        for row in table2.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_ReworkLost2 = pd.DataFrame(rows, columns=headers)

        
        output_path = os.path.join(output_dir, '17.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_ReworkLost2.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 17.csv") 
    else:
        print("No This Table")

# ---------------------------------------------------------------------------------------------------------
# 18

    if table5:
        headers = [header.text.strip() for header in table5.find_all('th')]
        rows = []
        for row in table5.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_ReworkLost5 = pd.DataFrame(rows, columns=headers) 

        output_path = os.path.join(output_dir, '18.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_ReworkLost5.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 18.csv")
    else:
        print("No This Table")

else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 65
url_65 = f'http://bppnet/report/pd/pdrwsumyr.aspx?yr{current_year}'
response = requests.get(url_65)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
            
        df_65 = pd.DataFrame(rows, columns=headers) 

        # Output file path
        output_path = os.path.join(output_dir, '65.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_65.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 65.csv")  
    else:
        print("No Table")
else:
    print(f"Page Not Fauls: {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 66-67

url_6667 = 'http://bppnet/energy/report/energy.aspx'
response = requests.get(url_6667)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView1'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_Electricity = pd.DataFrame(rows, columns=headers) 

         # Output file path
        output_path = os.path.join(output_dir, '66-67.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_Electricity.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 66-67.csv")  
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 68

url_68 = 'http://bppnet/energy/report/p2summary.aspx'
response = requests.get(url_68)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView2'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_LPG = pd.DataFrame(rows, columns=headers) 

         # Output file path
        output_path = os.path.join(output_dir, '68.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_LPG.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 68.csv")  
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 69
url_69 = 'http://bppnet/report/pd/pdpaint.aspx'
response = requests.get(url_69)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView2'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_paint = pd.DataFrame(rows, columns=headers) 

         # Output file path
        output_path = os.path.join(output_dir, '69.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_paint.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 69.csv")  
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 70

url_70 = 'http://bppnet/energy/report/energy.aspx'
response = requests.get(url_70)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView2'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_paintTon = pd.DataFrame(rows, columns=headers) 

         # Output file path
        output_path = os.path.join(output_dir, '70.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_paintTon.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 70.csv")  
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 72-73
url_7273 = 'http://bppnet/energy/report/energy.aspx'
response = requests.get(url_7273)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView3'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
        # สร้าง DataFrame จาก headers และ rows
        df_Water = pd.DataFrame(rows, columns=headers) 
        # Output file path
        output_path = os.path.join(output_dir, '72-73.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_Water.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 72-73.csv") 
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")

# ---------------------------------------------------------------------------------------------------------
# 77-78
url_7778 = 'http://bppnet/qm/report/ncstatus.aspx'

response = requests.get(url_7778)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView1'})  # ctl00_MainContent_datagrid1
   
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        # print("ข้อมูลถูกบันทึกลงในไฟล์ 77-78_Cust.csv")
        df_Cust = pd.DataFrame(rows, columns=headers) 

        # Output file path
        output_path = os.path.join(output_dir, '77-78.csv')
        
        # Create the directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the final DataFrame to CSV
        df_Cust.to_csv(output_path, index=False)
        
        # Print the final DataFrame
        print("ข้อมูลถูกบันทึกลงในไฟล์ 77-78.csv")  
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")
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