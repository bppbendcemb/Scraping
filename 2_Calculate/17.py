import pandas as pd
import os

df = pd.read_csv(r'F:\_BPP\Project\Scraping\1_Scraping\CSV\17.csv')

'การ Set Up เครื่อง/แม่พิมพ์ (MH)'
'จากเครื่องจักรเสีย (MH)'
'จากแม่พิมพ์เสีย (MH)'
'จากการรอวัสดุ/อุปกรณ์ (MH)'
'จากการไปผลิต/ทำงานอื่น (MH)'
'อื่น ๆ (MH) (ที่นอกเหนือหัวข้อข้างต้น)'
'MH ผลิตจริง (เฉพาะทีปิด)'




# Output file path
output_dir = r'F:\_BPP\Project\Scraping\2_Calculate\CSV'
output_path = os.path.join(output_dir, '17.csv')

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Save the final DataFrame to CSV
df.to_csv(output_path, index=False)

# Print the final DataFrame
print(df)