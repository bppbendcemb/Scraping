import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pandas as pd

current_year = datetime.now().year
# ------------------------------------------------------------------------------------------------
url = 'http://bppnet/report/kpi/kpipdrw.aspx'

response = requests.get(url)
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
        output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
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

        output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
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

        output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
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

