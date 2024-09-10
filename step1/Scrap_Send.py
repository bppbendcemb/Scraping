import os
import requests
from bs4 import BeautifulSoup
import csv
url = 'http://bppnet/report/whiss.aspx'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        folder_Output = 'Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'KPI_Send.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("ข้อมูลถูกบันทึกลงในไฟล์ KPI_Send.csv")
    else:
        print("ไม่พบตารางที่ระบุ")
else:
    print(f"ไม่สามารถเข้าถึงหน้าเว็บได้: {response.status_code}")       