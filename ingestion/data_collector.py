import pandas as pd
import os

data = {
    "text": [
        "I love this product",
        "This service is terrible",
        "Amazing experience",
        "Very bad quality",
        "I am happy with this purchase"
    ],
    "sentiment": [
        "positive",
        "negative",
        "positive",
        "negative",
        "positive"
    ]
}

df = pd.DataFrame(data)

# تحديد مسار المشروع
base_path = os.path.dirname(os.path.dirname(__file__))

# تحديد مسار data_lake/raw
data_lake_path = os.path.join(base_path, "data_lake", "raw", "raw_dataset.csv")

# إنشاء المجلد إذا لم يكن موجود
os.makedirs(os.path.dirname(data_lake_path), exist_ok=True)

# حفظ الملف
df.to_csv(data_lake_path, index=False)

print("Dataset saved successfully in data_lake/raw")