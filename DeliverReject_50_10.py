import os
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import pyodbc
import pandas as pd

url = 'http://bppnet/report/whiss.aspx'

response = requests.get(url)
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
        df_html = pd.DataFrame(rows, columns=headers)   

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'Deliver1.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("ข้อมูลถูกบันทึกลงในไฟล์ Deliver.csv")    

    else:
        print("No Table")
else:
    print(f"Page Not Found: {response.status_code}")

# ---------------------------------------------------------------------------------------

# เปลี่ยนชื่อคอลัมน์ใน DataFrame ที่ดึงมาจาก HTML
df_html.rename(columns={
    'ปี': 'yr',
    'เดือน': 'month',
    'จำนวนรายการ': 'items',
    'จำนวนชิ้นงาน': 'pieces'
}, inplace=True)

# แปลงคอลัมน์ 'month' เป็นชนิด int
df_html['month'] = df_html['month'].astype(int)

# ดึงค่า 'จำนวนชิ้นงาน' จากคอลัมน์
pieces_list = df_html[['month', 'pieces']].copy()

# แปลงค่า 'pieces' เป็น float และกรองค่าว่าง
pieces_list['pieces'] = pieces_list['pieces'].apply(lambda x: float(x.replace(',', '')) if x else pd.NA)

# ตรวจสอบว่ามี 12 เดือน ถ้าขาดให้เติม pd.NA
months_dict = {month: pd.NA for month in range(1, 13)}  # เตรียม dictionary ของเดือน
for index, row in pieces_list.iterrows():
    months_dict[row['month']] = row['pieces']  # ใส่ค่า 'pieces' ลงตามเดือนที่มีข้อมูล

# แปลง dictionary เป็น list ที่เรียงลำดับเดือน 1 ถึง 12
pieces_list_ordered = [months_dict[month] for month in range(1, 13)]

# สร้าง DataFrame ใหม่ที่มีเดือนเป็นหัวตาราง
months = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
df_deliver = pd.DataFrame(columns=['uniqueid', 'yr', 'kpi_id'] + months)

# เพิ่มข้อมูลลงใน DataFrame ใหม่
kpi_id = 50  # ค่า kpi_id คงที่
current_year = datetime.now().year
uniqueid = str(current_year) + str(kpi_id)  # สร้าง uniqueid

# สร้าง dictionary สำหรับแถวข้อมูลใหม่
new_row = {
    'uniqueid': uniqueid,
    'yr': current_year,
    'kpi_id': kpi_id,
    **dict(zip(months, pieces_list_ordered))
}

# ใช้ pd.concat เพื่อเพิ่มข้อมูลใหม่
df_deliver = pd.concat([df_deliver, pd.DataFrame([new_row])], ignore_index=True)

# แสดงผลลัพธ์
print(df_deliver)

# บันทึกเป็น CSV
df_deliver.to_csv('50.csv', index=False)
# --------------------------------------------------------------------------------------------------------------

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

        folder_Output = 'step1\Output'
        if not os.path.exists(folder_Output):
            os.makedirs(folder_Output)

        file_path = os.path.join(folder_Output, 'Reject.csv')    

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        print("ข้อมูลถูกบันทึกลงในไฟล์ Reject.csv")
    else:
        print("No This Table")
else:
    print(f"เข้าไม่ได้ : {response.status_code}")


# Convert 'วันที่เอกสาร' column to datetime format in Reject DataFrame
df_Reject['วันที่เอกสาร'] = pd.to_datetime(df_Reject['วันที่เอกสาร'], format='%d/%m/%Y')

# Clean and convert 'Reject' column from text to numbers
df_Reject['Reject'] = df_Reject['Reject'].astype(str).str.replace(',', '').str.strip()
df_Reject['Reject'] = pd.to_numeric(df_Reject['Reject'], errors='coerce')

# เปลี่ยนชื่อหลายคอลัมน์ใน DataFrame
rename_columns = {
    'วันที่เอกสาร': 'date',  # เปลี่ยนชื่อเป็น 'date'
    'สถานะเอกสาร': 'status',  # เปลี่ยนชื่อเป็น 'status'
}

df_Reject.rename(columns=rename_columns, inplace=True)

# เพิ่มคอลัมน์ 'm' และ 'yr'
df_Reject['m'] = df_Reject['date'].dt.month
df_Reject['yr'] = df_Reject['date'].dt.year

# เลือกเฉพาะคอลัมน์ที่ต้องการใช้งาน
selected_columns = ['yr', 'm', 'status', 'Reject']
df_selected = df_Reject[selected_columns]

# ลบแถวที่ status = 'ยกเลิก'
df_filtered = df_selected[df_selected['status'] != 'ยกเลิก']

# เลือกเฉพาะคอลัมน์ 'yr', 'm', และ 'Reject'
selected_columns2 = ['yr', 'm', 'Reject']
df_selected2 = df_filtered[selected_columns2]

# Group by 'yr' และ 'm' และรวมค่าในคอลัมน์ 'Reject'
df_grouped = df_selected2.groupby(['yr', 'm'], as_index=False)['Reject'].sum()

# Pivot table: เปลี่ยนค่าของ 'm' เป็นคอลัมน์ และแสดงผลรวมของ 'Reject'
df_pivot = df_grouped.pivot_table(index='yr', columns='m', values='Reject', aggfunc='sum').reset_index()

# Create a DataFrame with all months (1-12)
all_months = pd.DataFrame(0, index=range(len(df_pivot)), columns=range(1, 13))  # Create columns for months 1-12
all_months['yr'] = df_pivot['yr']  # Add 'yr' column from df_pivot

# Fill in the values from df_pivot
for month in range(1, 13):
    if month in df_pivot.columns:
        all_months[month] = df_pivot[month].fillna(0)  # Fill NaN with 0

# Reorder columns to move 'yr' to the leftmost position
all_months = all_months[['yr'] + [col for col in all_months.columns if col != 'yr']]

# Rename columns
rename_dict = {
    1: 'm01',
    2: 'm02',
    3: 'm03',
    4: 'm04',
    5: 'm05',
    6: 'm06',
    7: 'm07',
    8: 'm08',
    9: 'm09',
    10: 'm10',
    11: 'm11',
    12: 'm12'
}

kpi_id = [10]
all_months.insert(loc=1, column='kpi_id', value=kpi_id)


# Check if columns to rename exist in all_months
if set(rename_dict.keys()).issubset(all_months.columns):
    all_months.rename(columns=rename_dict, inplace=True)
    
else:
    print("One or more columns to rename do not exist in all_months")

df_Reject = all_months
# df_Reject.to_csv('10.csv', index=False)

# --------------------------------------------------------------------------------------------------------------
# 10.Reject Calculate

# Union of the two DataFrames
df_union = pd.concat([df_Reject, df_deliver]).drop_duplicates().reset_index(drop=True)

# Filter for specific kpi_id
filtered_df = df_union[df_union['kpi_id'].isin([10, 50])]

# Save the filtered DataFrame to CSV
# filtered_df.to_csv('filtered_df.csv', encoding='utf-8-sig', index=False)

# Get the values for kpi_id=50
kpi_50 = filtered_df[filtered_df['kpi_id'] == 50]

# Create a list to store results
results_list = []
yr_value = filtered_df['yr'].unique()[0]
# Calculate the ratio for each month
for month in range(1, 13):
    try:
        kpi_10_value = filtered_df[filtered_df['kpi_id'] == 10][f'm{month:02}'].values[0]  # Use m01, m02, ...
        kpi_50_value = kpi_50[f'm{month:02}'].values[0]  # Value for kpi_id=50 for that month

        # Calculate the ratio
        if kpi_50_value != 0:  # Check to avoid division by zero
            ratio = (kpi_10_value * 1000000) / kpi_50_value
        else:
            ratio = None  # If kpi_50_value is 0, set ratio to None

        # Add the data to the results list
        results_list.append({'yr': yr_value, 'kpi_id': 10, 'month': month, 'ratio': ratio})

    except IndexError:
        print(f"No data for month {month} for kpi_id 10 or 50.")
        continue

# Convert results list to DataFrame
result = pd.DataFrame(results_list)

# Add 'yr' and 'kpi_id' columns
result['yr'] = yr_value
result['kpi_id'] = 10
# Create 'uniqueid' column by concatenating 'yr' and 'kpi_id'
result['uniqueid'] = result['yr'].astype(str) + result['kpi_id'].astype(str)

# Reorder columns to place 'yr', 'kpi_id', and 'uniqueid' at the front
result = result[['uniqueid', 'yr', 'kpi_id', 'month', 'ratio']]

# Pivot the DataFrame to make it horizontal
result_pivot = result.pivot(index=['uniqueid','yr', 'kpi_id'], columns='month', values='ratio').reset_index()

# Rename columns for clarity
result_pivot.columns.name = None  # Remove the name of the columns
result_pivot.columns = [f'm{int(col):02}' if isinstance(col, int) else col for col in result_pivot.columns]

# Display the result DataFrame
print("Result DataFrame (Horizontal):")
print(result_pivot)

# Optional: Save the horizontal result DataFrame to CSV

df_Reject = result_pivot
df_Reject.to_csv('10.csv', encoding='utf-8-sig', index=False)

#__________________________________________________________


# ตรวจสอบการเชื่อมต่อกับ SQL Server
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)
    exit()

cursor = conn.cursor()

numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
# แปลงคอลัมน์ตัวเลขเป็น float และตรวจสอบค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    df_Reject[column] = pd.to_numeric(df_Reject[column], errors='coerce')
    if df_Reject[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_Reject[column] = df_Reject[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_Reject.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_Reject.iterrows():
    cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
    result = cursor.fetchone()[0]
    
    if result > 0:
        # อัปเดตข้อมูล
        sql = """
        UPDATE KPI_dtl
        SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
            m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
        WHERE unique_id = ? 
        """      
        cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                       row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                       row['m11'], row['m12'], row['yr'], row['uniqueid'])
        print("update")
    else:
        # แทรกข้อมูลใหม่
        sql = """
        INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        cursor.execute(sql, row['uniqueid'], row['yr'], 10,  # เปลี่ยน row['id'] เป็น 10
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()

# -----------------------------------------------------------------------------------------

# ตรวจสอบการเชื่อมต่อกับ SQL Server
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=c259-003\\SQLEXPRESS;'
        'DATABASE=KPI;'
        'Trusted_Connection=yes;'
    )
except pyodbc.Error as e:
    print("Error connecting to SQL Server:", e)
    exit()

cursor = conn.cursor()

numeric_columns = ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']
# แปลงคอลัมน์ตัวเลขเป็น float และตรวจสอบค่าที่ไม่ถูกต้อง
for column in numeric_columns:
    df_deliver[column] = pd.to_numeric(df_deliver[column], errors='coerce')
    if df_deliver[column].isnull().any():
        print(f"Warning: Column {column} contains invalid values. They will be replaced with 0.0.")
        df_deliver[column] = df_deliver[column].fillna(0.0)

# ตรวจสอบข้อมูลก่อนที่จะส่งไปยังฐานข้อมูล
print(df_deliver.head())

# ดำเนินการแทรกหรืออัปเดตข้อมูลใน SQL Server
for index, row in df_deliver.iterrows():
    cursor.execute("SELECT COUNT(*) FROM KPI_dtl WHERE unique_id = ?", row['uniqueid'])
    result = cursor.fetchone()[0]
    
    if result > 0:
        # อัปเดตข้อมูล
        sql = """
        UPDATE KPI_dtl
        SET m01 = ?, m02 = ?, m03 = ?, m04 = ?, m05 = ?, m06 = ?, m07 = ?, m08 = ?, 
            m09 = ?, m10 = ?, m11 = ?, m12 = ?, yr = ?, update_date = GETDATE()
        WHERE unique_id = ? 
        """      
        cursor.execute(sql, row['m01'], row['m02'], row['m03'], row['m04'], row['m05'], 
                       row['m06'], row['m07'], row['m08'], row['m09'], row['m10'], 
                       row['m11'], row['m12'], row['yr'], row['uniqueid'])
        print("update")
    else:
        # แทรกข้อมูลใหม่
        sql = """
        INSERT INTO KPI_dtl (unique_id, yr, kpi_id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, create_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        cursor.execute(sql, row['uniqueid'], row['yr'], 50,  # เปลี่ยน row['id'] เป็น 10
                       row['m01'], row['m02'], row['m03'], row['m04'], 
                       row['m05'], row['m06'], row['m07'], row['m08'], 
                       row['m09'], row['m10'], row['m11'], row['m12'])
        print("create")

# บันทึกการเปลี่ยนแปลง
conn.commit()

# ปิดการเชื่อมต่อ
cursor.close()
conn.close()