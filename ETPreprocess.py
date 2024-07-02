from ETRequest import ETRequest
from Queue import Queue

import json
import logging
import pandas as pd

class ETPreprocess:
	def __init__(self, fields_queue: Queue, points_ref: any) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
		self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time', 'et_actual', 'et_forecast'])
	
	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
	
	def start(self, ts_endpoint: str, fc_endpoint: str, logger: logging.Logger = None) -> int:
		'''Begins gathering ET data from timeseries and forecast endpoints'''
		failed_fields = 0
		while self.fields_queue.is_empty() is False:
			timeseries_success = False
			forecast_success = False
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			current_crop = self.points_ref['CROP_2020'][current_field_id]
			
			if logger is not None: logger.info(f"Now analyzing field ID {current_field_id}")
			# Fetch timeseries data
			timeseries_arg = {
					"date_range": [
						"2023-01-01", "2023-12-31"
					],
					"interval": "daily",
					"geometry": current_point_coordinates,
					"model": "Ensemble",
					"units": "mm",
					"variable": "ET",
					"reference_et": "gridMET",
					"file_format": "JSON"
				}
			
			timeseries_res = ETRequest(ts_endpoint, timeseries_arg)
			timeseries_res.send(logger=logger)
			timeseries_success = timeseries_res.success()
			# Fetch forecasted data
			forecast_arg = {
					"date_range": [
						"1980-01-01", "2023-06-03"
					],
					"interval": "daily",
					"geometry": current_point_coordinates,
					"model": "Ensemble",
					"units": "mm",
					"variable": "ET",
					"reference_et": "gridMET",
					"file_format": "JSON"
				}
		
			forecast_res = ETRequest(fc_endpoint, forecast_arg)
			forecast_res.send(logger=logger)
			forecast_success = forecast_res.success()
			# If both are successful, store it!
			if timeseries_success and forecast_success:
				# Data returns as a list[12] for each
				# Each item is of dict{'time': str, 'et': float}
				timeseries_content = json.loads(timeseries_res.response.content.decode('utf-8'))
				timeseries_data = [data for data in timeseries_content]

				forecast_content = json.loads(forecast_res.response.content.decode('utf-8'))
				forecast_data = [data for data in forecast_content]
    
				for item in timeseries_data:
					try:
						entry_time = item['time']
						entry_et_actual = item['et']
						entry_et_forecast = forecast_data[timeseries_data.index(item)]['et']
		
						self.data_table = pd.concat([pd.DataFrame([[current_field_id, current_crop, entry_time, entry_et_actual, entry_et_forecast]], columns=self.data_table.columns), self.data_table], ignore_index=True)
					except:
						forecast_index = timeseries_data.index(item)
						print(f'field: {current_field_id}')
						print(f'ts: {len(timeseries_data)}; fc: {len(forecast_data)}')
						print(f'{forecast_index}/{len(forecast_data)}')
						exit()
				if logger is not None: logger.info("Successful")
			else:
				if logger is not None: logger.warning(f"Analyzing for {current_field_id} failed")
				failed_fields+=1
	
			self.fields_queue.dequeue()
			if logger is not None: logger.info(f"{str(self.fields_queue.size())} fields remaining")
		
  		# Wierdly enough, presetting the index will cause a failure. So set index after completion.
		self.data_table.set_index('field_id', inplace=True)
		return failed_fields