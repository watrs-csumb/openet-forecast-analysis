# -*- coding: utf-8 -*-
"""
Created on Thu Jun  13 10:06:44 2024

@author: Robin Fishman
"""

from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta
from dotenv import dotenv_values
from src import CloudStorage, ETFetch, ETArg, Authenticate
from pathlib import Path

import logging
import pandas as pd
import sys

# LOGGING CONFIG
# File handler that allows files to show all log entries
file_log_handler = logging.FileHandler(
    filename=datetime.now().strftime("logs/main_%Y_%m_%d_%H_%M_%S.log")
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

api_key = dotenv_values(".env").get("ET_KEY")
timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
polygon_timeseries_endpoint = (
    "https://developer.openet-api.org/raster/timeseries/polygon"
)

forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"
polygon_forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal_polygon"

kern_polygon_fields = pd.read_csv("./data/kern_polygons.csv", low_memory=False)
monterey_polygon_fields = pd.read_csv("./data/monterey_polygons.csv", low_memory=False)
cali_fields = pd.concat([kern_polygon_fields, monterey_polygon_fields]).set_index("OPENET_ID")

finney_polygon_fields = pd.read_csv("./data/finney_county_ks.csv", low_memory=False).set_index("OPENET_ID")

def get_historical_data(
    fields_queue,
    reference,
    *,
    filename,
    end_date: str,
    endpoint=timeseries_endpoint,
    overpass=False,
    use_cloud: bool | CloudStorage = False,
):
    et_data = ETFetch(
        deepcopy(fields_queue),
        reference,
        api_key=api_key,  # type: ignore
    )

    timeseries_et = ETArg(
        "actual_et",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", end_date],
            "variable": "ET",
            "reducer": "mean",
            "overpass": overpass
        },
    )

    timeseries_eto = ETArg(
        "actual_eto",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", end_date],
            "variable": "ETo",
            "reducer": "mean"
        },
    )

    timeseries_etof = ETArg(
        "actual_etof",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", end_date],
            "variable": "ETof",
            "reducer": "mean"
        },
    )
    timeseries_ndvi = ETArg(
        "ndvi",
        args={
            "endpoint": endpoint,
            "date_range": ["2016-01-01", end_date],
            "variable": "ndvi",
            "reducer": "mean"
        }
    )

    arg_list = [timeseries_et]
    if not overpass:
        arg_list = arg_list + [timeseries_eto, timeseries_etof, timeseries_ndvi]

    et_data.start(
        request_args=arg_list,
        frequency="daily",
        logger=logger,
        packets=True,
    )

    if isinstance(use_cloud, CloudStorage):
        use_cloud.fetch_save(et_data, f"{filename}.parquet", parents=True)
    else:
        et_data.export(f"data/{filename}.parquet", file_format="pq")
    if overpass:
        return
    # Climatology compilation
    et_data.data_table["time"] = pd.to_datetime(et_data.data_table["time"])
    # Create a column for day of year
    et_data.data_table["doy"] = et_data.data_table["time"].dt.dayofyear
    # Group by field, crop, and doy then calculate the average conditions
    climatology_table = et_data.data_table.groupby(["field_id", "crop", "doy"])[
        ["actual_et", "actual_eto", "actual_etof"]
    ].agg("mean")
    
    if isinstance(use_cloud, CloudStorage):
        fname = f"{filename}_climatology.csv"
        climatology_table.reset_index().to_csv(f"data/{fname}", index=False)
        use_cloud.pd_write(
            fname,
            climatology_table.reset_index(),
            index=False,
        )
    
    climatology_table.reset_index().to_csv(f"{filename}_climatology.csv", index=False)
    # End Climatology

    # Year-to-date Averages Compilation
    avgs_table = (
        et_data.data_table.loc[(et_data.data_table["time"].dt.year == 2024), :]
        .groupby(["field_id", "crop"])[["actual_et", "actual_eto", "actual_etof"]]
        .agg("mean")
    )
    if isinstance(use_cloud, CloudStorage):
        fname = f"{filename}_2024_avgs.csv"
        avgs_table.reset_index().to_csv(f'data/{fname}', index=False)

        use_cloud.pd_write(
            fname,
            climatology_table.reset_index(),
            index=False,
        )
    
    avgs_table.reset_index().to_csv(f"{filename}_2024_avgs.csv", index=False)
    # End Year-to-date Averages Compilation

def get_forecasts(
    fields_queue,
    reference,
    *,
    dir,
    end_date:str,
    endpoint=forecast_endpoint,
    polygon=False,
    align=False,
    use_cloud: bool | CloudStorage = False,
    make_parents=False,
    skip_exists=True
):
    # Gather predictions at weekly intervals.
    # Forecast begins predictions from the end_range. So to start predictions for Jan 1, set to Dec 31
    forecasting_date = datetime(2025, 4, 1)
    end_date_s = datetime.strptime(end_date, '%Y-%m-%d')
    interval_delta = timedelta(weeks=1)  # weekly interval

    # Create dir if it doesn't exist.
    file_dir = Path(f"data/forecasts/{dir}")
    if file_dir.exists() is False and make_parents:
        file_dir.mkdir(parents=True)

    logger.info("Getting forecast data.")
    while forecasting_date < end_date_s:
        process = ETFetch(
            deepcopy(fields_queue),
            reference,
            api_key=api_key,  # type: ignore
        )
        api_date_format = forecasting_date.strftime("%Y-%m-%d")
        filename = f"{file_dir}/{api_date_format}_forecast.parquet"
        
        # If skip_exists is True, skips operation if the output path already exists.
        if skip_exists and Path(filename).exists():
            print(f"{filename} already exists. Moving on..")
            forecasting_date = forecasting_date + interval_delta
            continue

        forecast_et = ETArg(
            "expected_et",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ET",
                "align": align
            },
        )

        forecast_eto = ETArg(
            "expected_eto",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ETo",
                "align": align,
            },
        )

        forecast_etof = ETArg(
            "expected_etof",
            args={
                "endpoint": endpoint,
                "date_range": ["2016-01-01", api_date_format],
                "variable": "ETof",
                "align": align,
            },
        )

        if polygon:
            forecast_et.reducer = "mean"
            forecast_eto.reducer = "mean"
            forecast_etof.reducer = "mean"

        logger.info(f"Forecasting from {api_date_format}")
        process.start(
            request_args=[forecast_et, forecast_eto, forecast_etof],
            frequency="daily",
            packets=True,
            logger=logger,
        )
        
        process.export(filename, file_format="pq")

        # If the use_cloud flag is a CloudStorage object, export to the bucket contained in the object.
        if isinstance(use_cloud, CloudStorage):
            try:
                use_cloud.pd_write(filename, process.export())
            except Exception:
                pass

        forecasting_date = forecasting_date + interval_delta

def main():
    if not api_key:
        print("Please set ET_KEY in the .env file.")
        sys.exit(1)
    
    storage_client = CloudStorage(
        "openet", credentials=Authenticate("./gapi_credentials.json"), logger=logger
    )
    # polygon forecasting
    ca_queue = deque(cali_fields.index.to_list())
    kansas_queue = deque(finney_polygon_fields.index.to_list())

    logger.info("Getting historical data")

    get_historical_data(
        ca_queue,
        cali_fields,
        filename="central_valley_historical",
        endpoint=polygon_timeseries_endpoint,
        overpass=False,
        use_cloud=storage_client,
        end_date='2025-09-20'
    )
    
    get_historical_data(
        kansas_queue,
        finney_polygon_fields,
        filename="finney_polygon_historical_overpass",
        endpoint=polygon_timeseries_endpoint,
        overpass=True,
        use_cloud=storage_client,
        end_date='2025-08-20'
    )
    
    logger.info("Getting DTW forecasts")
    get_forecasts(
        ca_queue,
        cali_fields,
        dir="dtw/central_valley/",
        endpoint=polygon_forecast_endpoint,
        polygon=True,
        use_cloud=storage_client,
        end_date="2025-09-30",
        align=True,
        make_parents=True
    )
    
    get_forecasts(
        kansas_queue,
        finney_polygon_fields,
        dir="dtw/kansas/",
        endpoint=polygon_forecast_endpoint,
        polygon=True,
        use_cloud=storage_client,
        end_date="2025-09-30",
        align=True,
        make_parents=True
    )


if __name__ == "__main__":
    main()
