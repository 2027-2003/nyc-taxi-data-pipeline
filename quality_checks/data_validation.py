import pandas as pd
import os
def validate_data():
    base_path = os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(base_path, "data_lake", "curated", "transformed_data.parquet")
    print("Checking data...")
    df = pd.read_parquet(path)
    errors = 0
    nulls = df.isnull().sum().sum()
    if nulls > 0:
        print(f"There are {nulls} null values")
        errors += 1
    else:
        print("No null values found")
    neg_prices = len(df[df["total_amount"] <= 0])
    if neg_prices > 0:
        print(f"There are {neg_prices} trips with negative price")
        errors += 1
    else:
        print("All prices are valid")
    bad_duration = len(df[(df["trip_duration_min"] <= 0)])
    if bad_duration > 0:
        print(f"There are {bad_duration} trips with invalid duration")
        errors += 1
    else:
        print("All durations are valid")
    print(f"\nTotal rows: {len(df):,}")
    print(f"Columns: {df.columns.tolist()}")
    if errors == 0:
        print("\nData is 100% clean — ready for dashboard!")
    else:
        print(f"\nThere are {errors} issues that need review")
if __name__ == "__main__":
    validate_data()