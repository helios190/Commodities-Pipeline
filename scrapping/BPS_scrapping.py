import requests
import pandas as pd

def fetch_and_transform_bps_data():
    url = 'https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/379/key/c943422ea45ef90be804642156963e2c'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
    datacontent = data.get('datacontent', {})
    tahun_mapping = {str(item["val"]): item["label"] for item in data.get("tahun", [])}
    turtahun_mapping = {str(item["val"]): item["label"] for item in data.get("turtahun", [])}
    
    def convert_time_period(time_period):
        year_code = time_period[5:8]
        month_code = time_period[8:]
        year = tahun_mapping.get(year_code, "Unknown Year")
        month = turtahun_mapping.get(month_code, "Unknown Month")
        return f"{year} {month}" if year != "Unknown Year" and month != "Unknown Month" else None

    df = pd.DataFrame(list(datacontent.items()), columns=['Time_Period', 'BI_Rate'])
    df['Date'] = df['Time_Period'].apply(convert_time_period)
    df_filtered = df[~df['Time_Period'].str.endswith("13")]
    
    return df_filtered

# Usage
df_result = fetch_and_transform_bps_data()
