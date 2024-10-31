import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

current_year = datetime.now().year
url = f'http://bppnet/report/pd/pdrwsumyr.aspx?yr{current_year}'
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table',{'id':'ctl00_MainContent_GridView1'})
    if table:
        headers = [header.text.strip() for header in table.find_all('th')]
        rows = []
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            rows.append([column.text.strip() for column in columns])

        # folder_Output = 'CSV'
        # if not os.path.exists(folder_Output):
        #     os.makedirs(folder_Output)

        # file_path = os.path.join(folder_Output, '65_CostRework.csv')    

        # with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
        #     writer = csv.writer(file)
        #     writer.writerow(headers)
        #     writer.writerows(rows)
        # print("Save To : 65_CostRework.csv")
         # สร้าง DataFrame จาก headers และ rows
        df_65 = pd.DataFrame(rows, columns=headers) 

        # Output file path
        output_dir = r'F:\_BPP\Project\Scraping\1_Scraping\CSV'
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