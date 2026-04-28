import pandas as pd
from sqlalchemy import create_engine
import os

def load_to_warehouse():
    engine = create_engine(
        "postgresql://postgres:1234@localhost:5432/nyc_taxi"
    )

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_path, "data_lake", "curated", "transformed_data.parquet")

    print("📂 جاري تحميل البيانات...")
    df = pd.read_parquet(path)

    df = df.rename(columns={
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "ratecode_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
    })

    print(f"📤 جاري رفع {len(df):,} صف...")

    # نرفع على دفعات يدوياً
    batch_size = 50000
    total = len(df)
    
    with engine.connect() as conn:
        # نحذف الجدول القديم ونبني جديد
        df.head(0).to_sql("taxi_trips", conn, if_exists="replace", index=False)
        conn.commit()
        
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            batch.to_sql("taxi_trips", conn, if_exists="append", index=False)
            conn.commit()
            print(f"✅ تم رفع {min(i+batch_size, total):,} من {total:,}")

    print("🎉 اكتمل الرفع بنجاح!")

if __name__ == "__main__":
    load_to_warehouse()