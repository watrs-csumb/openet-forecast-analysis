# units: inches
# vars: ET, ETo, EToF, count
# reference_et: cimis
# 1985-2024
# 468 files per band
from src import ETRequest
from datetime import datetime

from dotenv import dotenv_values

import time

endpoint = "https://developer.openet-api.org/raster/export/stack"
key = dotenv_values(".env").get('ET_KEY')

def export_stacks():
    year_start = 1985
    year_end = 2024
    
    while year_end >= year_start:
        print("Sending job for", year_end)
        range_start = datetime(year=year_end, month=1, day=1)
        range_end = datetime(year=year_end, month=12, day=31)
        
        request_arg = {
            "asset_id": "projects/watrs-rfishman/assets/colusa_yolo_subbasins",
            "date_range": [str(range_start.date()), str(range_end.date())],
            "variable": "ET",
            "reference_et": "gridMET",
            "units": "in",
            "model": "ensemble",
            "cog": False,
            "encrypt": False,
            "interval": "monthly",
        }
        
        req = ETRequest(endpoint, request_arg, key=key)
        
        req.send()
        
        year_end-=1
        time.sleep(2)

def main():
    export_stacks()
    return

if __name__ == '__main__':
    main()