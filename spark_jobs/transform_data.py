import pandas as pd
import os

base_path = os.path.dirname(os.path.dirname(__file__))

raw_path = os.path.join(base_path, "data_lake", "raw", "api_data.csv")

df = pd.read_csv(raw_path)

# اختيار الأعمدة المهمة
df = df[["userId", "id", "title"]]

# تنظيف النص
df["title"] = df["title"].str.lower()

processed_path = os.path.join(base_path, "data_lake", "processed", "processed_data.csv")

df.to_csv(processed_path, index=False)

print("Data transformed successfully")