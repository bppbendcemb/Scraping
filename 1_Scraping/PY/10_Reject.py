import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

current_year = datetime.now().year
# 10.Reject
url = f'http://bppnet/qm/report/ccrlst.aspx?ktype=yr&key={current_year}'
response = requests.get(url)
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
        output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
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
