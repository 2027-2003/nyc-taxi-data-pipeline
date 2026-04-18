import requests
import pandas as pd
import os

all_data = []

# جلب بيانات متعددة (زيادة الحجم)
for i in range(1, 6):
    url = f"https://jsonplaceholder.typicode.com/posts?_page={i}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        all_data.extend(data)
    else:
        print("Error fetching data")

df = pd.DataFrame(all_data)

base_path = os.path.dirname(os.path.dirname(__file__))
raw_path = os.path.join(base_path, "data_lake", "raw", "api_data.csv")

df.to_csv(raw_path, index=False)

print("Large API data ingested successfully")