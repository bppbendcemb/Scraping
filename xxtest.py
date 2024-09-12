def try_float(value):
    try:
        return float(value.replace(',', '')) if value else None
    except Exception as e:
        print(f"Error converting value to float: {value}, Error: {e}")
        return None

# Improved database operations with error handling
try:
    for row in data[1:]:  # Skip header row
        uniqueid = row[0]  # Unique ID in the first column

        cursor.execute(check_sql, uniqueid)
        exists = cursor.fetchone()[0]

        params = (
            row[2],  # kpi_id
            row[1],  # yr
            try_float(row[4]),  # m01
            try_float(row[5]),  # m02
            try_float(row[6]),  # m03
            try_float(row[7]),  # m04
            try_float(row[8]),  # m05
            try_float(row[9]),  # m06
            try_float(row[10]),  # m07
            try_float(row[11]),  # m08
            try_float(row[12]),  # m09
            try_float(row[13]),  # m10
            try_float(row[14]),  # m11
            try_float(row[15]),  # m12
        )

        if exists:
            cursor.execute(update_sql, (*params, uniqueid))
        else:
            cursor.execute(insert_sql, (*params,))

    conn.commit()
except Exception as e:
    print(f"An error occurred during database operations: {e}")
finally:
    cursor.close()
    conn.close()
