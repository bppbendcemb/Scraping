import pandas as pd

# โหลดข้อมูลจากไฟล์ CSV
df = pd.read_csv(r'F:\_BPP\Project\Scraping\step2\Output\Rework_new.csv')

# แปลงค่าจากคอลัมน์ m01 ให้เป็น float โดยลบเครื่องหมายจุลภาค
def convert_to_float(value):
    try:
        return float(str(value).replace(',', ''))
    except ValueError:
        return None

# ดึงค่า m01 สำหรับ kpi_id = 99
m01_99 = df.loc[df['kpi_id'] == 99, 'm01'].values[0]
m01_99 = convert_to_float(m01_99)

# ตรวจสอบว่าค่าที่ได้ไม่เป็น None และไม่เป็น 0 เพื่อหลีกเลี่ยงการหารด้วย 0
if m01_99 and m01_99 != 0:
    # คำนวณค่า m01 ถึง m12 สำหรับ kpi_id ทั้งหมดที่ไม่ใช่ 99
    for month in ['m01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']:
        df[month] = df.apply(
            lambda row: convert_to_float(row[month]) * 1000000 / m01_99 
            if row['kpi_id'] != 99 and pd.notnull(row[month]) 
            else convert_to_float(row[month]),  # ใช้ค่าเดิมสำหรับ kpi_id = 99
            axis=1
        )

    # แสดงผลลัพธ์
    print(df[['kpi_id', 'm01', 'm02', 'm03', 'm04', 'm05', 'm06', 'm07', 'm08', 'm09', 'm10', 'm11', 'm12']])
    
    # บันทึก DataFrame ลงในไฟล์ CSV ใหม่
    df.to_csv(r'F:\_BPP\Project\Scraping\step2\Output\Rework_new_calculated.csv', index=False)
else:
    print("ไม่สามารถหารด้วย 0 หรือข้อมูลไม่เพียงพอ")
