import requests
import pandas as pd

def fetch_bi_rate():
    url = 'https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/379/key/c943422ea45ef90be804642156963e2c'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")
    
    datacontent = data.get('datacontent', {})
    tahun_mapping = {str(item["val"]): item["label"] for item in data.get("tahun", [])}
    turtahun_mapping = {str(item["val"]): item["label"] for item in data.get("turtahun", [])}

    
    return datacontent, tahun_mapping, turtahun_mapping