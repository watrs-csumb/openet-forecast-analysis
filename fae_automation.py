from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from src import ETArg, ETFetch
from src.ETUtils import CloudStorage, Authenticate

import logging
import os
import pandas as pd
import sys
import time

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(
    filename=datetime.now().strftime(
        f"logs/{os.path.basename(__file__)}_%Y_%m_%d_%H_%M_%S.log"
    )
)

# Stream handler that prints log entries at level WARNING or higher
stdout_log_handler = logging.StreamHandler(stream=sys.stdout)
stdout_log_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[file_log_handler, stdout_log_handler],
)
logger = logging.getLogger(__name__)
# END LOGGING CONFIG

fae_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/fret_adjusted_et"
api_key = dotenv_values(".env").get("ET_KEY")
kern_fields = pd.read_csv("./data/kern_polygons.csv", low_memory=False).set_index(
    "OPENET_ID"
)
monterey_fields = pd.read_csv(
    "./data/monterey_polygons.csv", low_memory=False
).set_index("OPENET_ID")
# Drop fields with too large of polygons
monterey_fields.drop(index=["CA_244144", "CA_244402"], inplace=True)
kansas_fields = pd.read_csv("./data/finney_county_ks.csv", low_memory=False).set_index(
    "OPENET_ID"
)


def get_fae(
    data: pd.DataFrame, date_range: list[str], export_name: str, storage_client: CloudStorage
):
    if not api_key:
        raise ValueError("API key not found in environment variables")
    if data is None:
        raise ValueError("Data is empty")

    ff_et = ETArg(
        "ff_et",
        args={
            "endpoint": fae_endpoint,
            "date_range": date_range,
            "reference_et": "gridMET",
            "variable": "ET",
            "api_key": api_key,
            "reducer": "mean",
            "method": "forward_fill"
        },
    )
    avg_et = ETArg(
        "avg_et",
        args={
            "endpoint": fae_endpoint,
            "date_range": date_range,
            "reference_et": "gridMET",
            "variable": "ET",
            "api_key": api_key,
            "reducer": "mean",
            "method": "climatology"
        },
    )
    med_et = ETArg(
        "med_et",
        args={
            "endpoint": fae_endpoint,
            "date_range": date_range,
            "reference_et": "gridMET",
            "variable": "ET",
            "api_key": api_key,
            "reducer": "mean",
            "method": "climatology_median"
        },
    )

    queue = deque(data.index.to_list())
    job = ETFetch(deepcopy(queue), data, api_key=api_key)

    job.start(
        request_args=[ff_et, avg_et, med_et],
        frequency="daily",
        packets=True,
        logger=logger
    )
    
    storage_client.fetch_save(
        job, f"forecasts/fae/{export_name}", parents=True
    )


def main():
    check_time = datetime.now()
    check_interval = timedelta(days=6)
    upcoming_check_time = check_time + check_interval
    run_fetch = True
    date_range = [
        datetime.now().strftime("%Y-%m-%d"), 
        (datetime.now() + timedelta(weeks=1)).strftime("%Y-%m-%d")
        ]
    storage_client = CloudStorage("openet", Authenticate("./gapi_credentials.json"), logger=logger)
    
    try:
        while True:
            if run_fetch:
                #-- Monterey --#
                get_fae(monterey_fields, date_range, f"monterey/fae_{date_range[0]}", storage_client)
                
                #-- Kern --#
                get_fae(kern_fields, date_range, f"kern/fae_{date_range[0]}", storage_client)
                
                #-- Kansas --#
                get_fae(kansas_fields, date_range, f"kansas/fae_{date_range[0]}", storage_client)
                
                logger.info(
                    f"FAE automation finished on: {check_time}. Next check at: {upcoming_check_time}"
                )
                
                run_fetch = False
                continue
        
            if datetime.now() >= upcoming_check_time:
                check_time = datetime.now()
                upcoming_check_time = check_time + check_interval
                date_range = [
                    datetime.now().strftime("%Y-%m-%d"), 
                    (datetime.now() + timedelta(weeks=1)).strftime("%Y-%m-%d")
                    ]
                run_fetch = True
            else:
                time.sleep(60)
            
    except KeyboardInterrupt:
        return
    except Exception as err:
        logger.exception("FAE automation failed. " + str(err))
        sys.exit(1)

if __name__ == "__main__":
    main()