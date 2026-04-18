import pandas as pd
import psycopg2
import os

base_path = os.path.dirname(os.path.dirname(__file__))

processed_path = os.path.join(base_path, "data_lake", "processed", "processed_data.csv")

df = pd.read_csv(processed_path)

conn = psycopg2.connect(
    host="localhost",
    port="1234",        # ✅ مهم جدًا
    database="ai_data_platform",
    user="postgres",
    password="12345"    # ✅ الباسورد الصح
)

cursor = conn.cursor()

for index, row in df.iterrows():
    cursor.execute(
        "INSERT INTO posts (userid, id, title) VALUES (%s, %s, %s)",
        (row["userId"], row["id"], row["title"])
    )

conn.commit()

cursor.close()
conn.close()

print("Data loaded into warehouse successfully")