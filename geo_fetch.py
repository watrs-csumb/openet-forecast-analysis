from dotenv import dotenv_values

import gzip
import pandas as pd
import requests

api_key = dotenv_values(".env").get("ET_KEY")

boundaries_endpoint = "https://developer.openet-api.org/geodatabase/metadata/boundaries"

monterey_fields = pd.read_csv("./Monterey.csv", low_memory=False).set_index("OPENET_ID")

def main():
    data = requests.post(
        headers={"Authorization": api_key},
        json={"field_ids":  monterey_fields.index.tolist()},
		url=boundaries_endpoint)
    
    data = eval(gzip.decompress(data.content).decode())
    
if __name__ == "__main__":
    main()