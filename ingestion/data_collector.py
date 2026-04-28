import requests
import os

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

def download_data():
    base_path = os.path.dirname(os.path.dirname(__file__))
    save_path = os.path.join(base_path, "data_lake", "raw", "yellow_tripdata_2023-01.parquet")
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    print("جاري تحميل بيانات NYC Taxi الحقيقية...")
    
    response = requests.get(URL, stream=True)
    total = 0
    
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            total += len(chunk)
            print(f"تم تحميل: {total / 1024 / 1024:.1f} MB", end="\r")
    
    print(f"\n✅ تم! الحجم الكلي: {total / 1024 / 1024:.1f} MB")
    print(f"📁 محفوظ في: {save_path}")

if __name__ == "__main__":
    download_data()