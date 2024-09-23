import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pandas as pd

current_year = datetime.now().year
url = f'http://bppnet/qm/report/ccrlst.aspx?ktype=yr&key={current_year}'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'MainContent_datagrid1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'Reject1.csv')
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
           writer = csv.writer(file)
           writer.writerow(headers)
           writer.writerows(rows)
        print("บันทึกไฟล์ชื่อ : Reject1.csv")
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")       
#______________________________________________________

# Processing the CSV files using pandas
input_file = os.path.join(folder_Output, 'Reject1.csv')
output_file = os.path.join(folder_Output, 'Reject2.csv')
# input_deliver = os.path.join(folder_Output, 'Deliver2.csv')

# Read the CSV files
df = pd.read_csv(input_file)
# df_deliver = pd.read_csv(input_deliver)

# Convert 'วันที่เอกสาร' column to datetime format in Reject DataFrame
df['วันที่เอกสาร'] = pd.to_datetime(df['วันที่เอกสาร'], format='%d/%m/%Y')

# Clean and convert 'Reject' column from text to numbers
df['Reject'] = df['Reject'].astype(str).str.replace(',', '').str.strip()
df['Reject'] = pd.to_numeric(df['Reject'], errors='coerce')


df.to_csv(output_file, encoding='utf-8-sig')