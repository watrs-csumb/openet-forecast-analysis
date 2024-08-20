# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 09:39:58 2024

@author: Robin Fishman
"""

import pandas as pd
import gzip
import requests

field_props_endpoint = "https://developer.openet-api.org/geodatabase/metadata/properties"

field_df = pd.read_csv('./data/Kern.csv', low_memory=False).set_index('OPENET_ID')

header = {"Authorization": "5uich75YbcZBS7h8Fg2JCDvY7dlc7pFDMYjzgwByTBcUrt3sXaYY7pZVRC6w"}
args = {
	"field_ids": field_df.index.to_list()
}
res = requests.post(headers=header, json=args, url=field_props_endpoint)

data = eval(gzip.decompress(res.content).decode())


