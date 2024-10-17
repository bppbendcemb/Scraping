import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pyodbc
import pandas as pd

current_year = datetime.now().year
url = 'http://bppnet/energy/report/energy.aspx'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView3'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])
 # สร้าง DataFrame จาก headers และ rows
        df = pd.DataFrame(rows, columns=headers)

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'SprayPaintTon.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("Save To : SprayPaintTon.csv")
    else:
        print("No Table")
else:
    print(f"Page Not Fauls: {response.status_code}")