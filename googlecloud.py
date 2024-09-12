from google.cloud import storage
from google.oauth2 import service_account
import json

# กำหนดเส้นทางไปยังไฟล์คีย์ JSON
key_path = "F:/_BBEAR/condo-test-web-3775172353ef.json"

# สร้าง credentials จากไฟล์คีย์
credentials = service_account.Credentials.from_service_account_file(key_path)

# สร้าง client สำหรับ Cloud Storage โดยใช้ credentials
client = storage.Client(credentials=credentials)

# กำหนดชื่อ Bucket และชื่อไฟล์ JSON ที่ต้องการดึง
bucket_name = 'bbear'
blob_name = 'company.json'

# เข้าถึง Bucket และ Blob
bucket = client.get_bucket(bucket_name)
blob = bucket.blob(blob_name)

# ดึงข้อมูล JSON จาก Blob
json_data = blob.download_as_text()

# แปลง JSON เป็นอ็อบเจ็กต์ Python
data = json.loads(json_data)

# แสดงข้อมูล JSON
print(json.dumps(data, indent=4, ensure_ascii=False))
