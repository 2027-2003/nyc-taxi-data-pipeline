import pandas as pd
import os

base_path = os.path.dirname(os.path.dirname(__file__))

processed_path = os.path.join(base_path, "data_lake", "processed", "processed_data.csv")

df = pd.read_csv(processed_path)

# فحص القيم الفارغة
if df.isnull().values.any():
    print("Dataset contains missing values")
else:
    print("No missing values found")

# فحص عدد الصفوف
print("Total rows:", len(df))

# فحص الأعمدة
print("Columns:", df.columns.tolist())

print("Data validation completed successfully")