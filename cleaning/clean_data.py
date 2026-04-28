import pandas as pd
import os
def clean_data():
    base_path = os.path.dirname(os.path.dirname(__file__))
    raw_path = os.path.join(base_path, "data_lake", "raw", "yellow_tripdata_2023-01.parquet")
    clean_path = os.path.join(base_path, "data_lake", "processed", "cleaned_data.parquet")
    os.makedirs(os.path.dirname(clean_path), exist_ok=True)
    
    print("Loading data...")
    df = pd.read_parquet(raw_path)

    print(f"Number of rows before cleaning: {len(df):,}")
    df = df.dropna()

    print(f"After removing nulls: {len(df):,}")
    df = df[df["total_amount"] > 0]

    print(f"After removing negative price: {len(df):,}")
    df = df[df["trip_distance"] > 0]

    print(f"After removing zero distance: {len(df):,}")
    df = df[df["passenger_count"] > 0]

    print(f"After removing invalid passengers: {len(df):,}")
    df.to_parquet(clean_path, index=False)

    print(f"\nSaved to: {clean_path}")
    print(f"Total cleaned rows: {len(df):,}")
if __name__ == "__main__":
    clean_data()