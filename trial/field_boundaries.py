import gzip
import json
import requests
from dotenv import dotenv_values

keys = dotenv_values(".env")
header = {"Authorization": keys.get('ET_KEY')}

# endpoint arguments
args = {
  "field_ids": [
    "06183913",
    "06208981"
  ]
}

# query the api 
resp = requests.post(
    headers=header,
    json=args,
    url="https://developer.openet-api.org/geodatabase/metadata/boundaries"
)

# unzip the data
data = eval(gzip.decompress(resp.content).decode())

with open('boundaries.json', 'w') as boundaries_file:
    writer = json.dump(data, boundaries_file, indent=4)

