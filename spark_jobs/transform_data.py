import pandas as pd
import os

def transform_data():
    base_path = os.path.dirname(os.path.dirname(__file__))
    clean_path = os.path.join(base_path, "data_lake", "processed", "cleaned_data.parquet")
    output_path = os.path.join(base_path, "data_lake", "curated", "transformed_data.parquet")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print("📂 جاري تحميل البيانات النظيفة...")
    df = pd.read_parquet(clean_path)

    # 1. إضافة عمود مدة الرحلة بالدقائق
    df["trip_duration_min"] = (
        pd.to_datetime(df["tpep_dropoff_datetime"]) - 
        pd.to_datetime(df["tpep_pickup_datetime"])
    ).dt.total_seconds() / 60

    # 2. إضافة عمود سعر الكيلومتر
    df["price_per_mile"] = df["total_amount"] / df["trip_distance"]

    # 3. إضافة عمود ساعة الرحلة
    df["pickup_hour"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.hour

    # 4. حذف الرحلات بمدة غير منطقية
    df = df[(df["trip_duration_min"] > 1) & (df["trip_duration_min"] < 180)]

    df.to_parquet(output_path, index=False)
    print(f"✅ تم التحويل! عدد الصفوف: {len(df):,}")
    print(f"📁 محفوظ في: {output_path}")

if __name__ == "__main__":
    transform_data()