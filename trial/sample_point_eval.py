import json
import pandas
import requests
from dotenv import dotenv_values

sample_points = pandas.read_csv('../sample_points.csv', low_memory=False)

sample_points_timeseries = {}

timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"

header = {"Authorization": dotenv_values("../.env").get("ET_KEY")}

for point in sample_points.index:
    # point: [OPENET_ID, CROP_2020, .geo]
    point_coordinates = json.loads(sample_points['.geo'][point])['coordinates']
    
    timeseries_arg = {
        "date_range": [
            "2023-01-01", "2023-12-31"
    	],
    	"interval": "monthly",
        "geometry": point_coordinates,
        "model": "Ensemble",
		"units": "mm",
		"variable": "ET",
		"reference_et": "gridMET",
		"file_format": "JSON"
    }
    
    timeseries_res = requests.post(
		headers=header,
		url=timeseries_endpoint,
		json=timeseries_arg
	)
    
    sample_points_timeseries[sample_points['OPENET_ID'][point]] = json.loads(timeseries_res.content.decode('utf-8'))
    
    # todo: same thing as above but for forecasting

sample_points_timeseries = pandas.DataFrame(sample_points_timeseries)
