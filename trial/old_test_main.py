import requests
from dotenv import dotenv_values

keys = dotenv_values("../.env")
header = {"Authorization": keys.get('ET_KEY')}

# Timeseries Actual
timeseries_endpoint = "https://developer.openet-api.org/raster/timeseries/point"
tsa_filename = "timeseries_actual.csv"

args = {
	"date_range": [
		"2023-01-01", "2023-12-31"
	],
	"interval": "monthly",
	"geometry": [
		-121.36322,
    	38.87626
	],
	"model": "Ensemble",
	"units": "mm",
	"variable": "ET",
	"reference_et": "gridMET",
	"file_format": "CSV"
}

tsa_resp = requests.post(
	headers=header,
	json=args,
	url=timeseries_endpoint
)

tsa_file = open(tsa_filename, 'wb')
tsa_file.write(tsa_resp.content)
tsa_file.close()

# Timeseries Forecast
forecast_endpoint = "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal"
forecast_filename = "timeseries_forecast_seasonal.csv"

forecast_args = {
	"date_range": [
		"2016-01-01",
    	"2023-06-03"
  	],
	"file_format": "CSV",
	"geometry": [
		-121.36322,
		38.87626
	],
	"interval": "monthly",
	"model": "Ensemble",
	"reference_et": "gridMET",
	"units": "mm",
	"variable": "ET"
}

forecast_resp = requests.post(
	headers=header,
	json=forecast_args,
	url=forecast_endpoint
)

forecast_file = open(forecast_filename, 'wb')
forecast_file.write(forecast_resp.content)
forecast_file.close()
