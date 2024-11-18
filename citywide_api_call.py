import os
import json
import requests


base_url = os.getenv('CW_API_ENDPOINT')
token= os.getenv('CW_API_TOKEN')
endpoint = "assets" #list of assets

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json"
}

params = {} # if any

response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)

if response.status_code == 200:
    try:
        assets_data = response.json()
        print("Assets data downloaded successfully.")
        
        with open("assets_data.json", "w") as json_file:
            json.dump(assets_data, json_file, indent=4)
        print("Assets data saved to assets_data.json")

    except json.JSONDecodeError:
        print("Failed to decode JSON response.")
else:
    print(f"Failed to fetch assets. Status code: {response.status_code}, Response: {response.text}")
