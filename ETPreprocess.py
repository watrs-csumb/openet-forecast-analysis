from ETRequest import ETRequest
from Queue import Queue

import json

class ETPreprocess:
	def __init__(self) -> None:
		pass
	
	def __init__(self, fields_queue: Queue, points_ref: any) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
	
	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
	
	def start(self, ts_endpoint: str, fc_endpoint: str) -> int:
		failed_fields = 0
		while self.fields_queue.is_empty() is False:
			timeseries_success = False
			forecast_success = False
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			
			print("[LOG] Now analyzing field ID " + current_field_id)
			# Fetch timeseries data
			timeseries_arg = {
					"date_range": [
						"2023-01-01", "2023-12-31"
					],
					"interval": "monthly",
					"geometry": current_point_coordinates,
					"model": "Ensemble",
					"units": "mm",
					"variable": "ET",
					"reference_et": "gridMET",
					"file_format": "JSON"
				}
			
			timeseries_res = ETRequest(ts_endpoint, timeseries_arg)
			timeseries_success = timeseries_res.success()
			# Fetch forecasted data
			forecast_arg = {
					"date_range": [
						"2016-01-01", "2023-12-31"
					],
					"interval": "monthly",
					"geometry": current_point_coordinates,
					"model": "Ensemble",
					"units": "mm",
					"variable": "ET",
					"reference_et": "gridMET",
					"file_format": "JSON"
				}
		
			forecast_res = ETRequest(fc_endpoint, forecast_arg)
			forecast_success = forecast_res.success()
			# If both are successful, store it!
			if timeseries_success and forecast_success:
				print("[LOG] Successful")
			else:
				print("[WARN] Analyzing for " + current_field_id + " failed")
				failed_fields+=1
	
			self.fields_queue.dequeue()
			print("[LOG] " + str(self.fields_queue.size()) + " fields remaining")

		return failed_fields