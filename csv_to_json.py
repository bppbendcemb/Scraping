import pandas as pd

# อ่านไฟล์ CSV
csv_file_path = 'F:/_BBEAR/Data/BBEAR/BBEAR/company.csv'  # แก้ไขเป็นที่อยู่ของไฟล์ CSV ของคุณ
data = pd.read_csv(csv_file_path)

# แปลงข้อมูลเป็น JSON
json_data = data.to_json(orient='records', force_ascii=False, lines=False)

# บันทึกข้อมูล JSON ลงไฟล์
json_file_path = 'F:/_BBEAR/Data/BBEAR/BBEAR/company.json'  # แก้ไขเป็นที่อยู่ที่คุณต้องการบันทึกไฟล์ JSON
with open(json_file_path, 'w', encoding='utf-8') as json_file:
    json_file.write(json_data)

print("Conversion completed! JSON data saved to:", json_file_path)
