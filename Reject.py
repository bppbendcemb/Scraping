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

        file_path = os.path.join(folder_Output, 'Reject.csv')
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
           writer = csv.writer(file)
           writer.writerow(headers)
           writer.writerows(rows)
        print("บันทึกไฟล์ชื่อ : Reject.csv")
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")       
#______________________________________________________

df = pd.read_csv(f'F:\_BPP\Project\Scraping\step1\Output\Reject.csv')

# แปลงคอลัมน์ 'วันที่เอกสาร' ให้เป็นรูปแบบ datetime
df['วันที่เอกสาร'] = pd.to_datetime(df['วันที่เอกสาร'], format='%d/%m/%Y')
# Group by เดือนและหาผลรวมของ Reject
monthly_reject = df.groupby(df['วันที่เอกสาร'].dt.month)['Reject'].sum()
print(monthly_reject)


df.to_csv(monthly_reject, index=False, encoding='utf-8-sig')