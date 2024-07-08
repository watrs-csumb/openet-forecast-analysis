from ETRequest import ETRequest
from ETArg import ETArg
from Queue import Queue
from typing import Union, List, Tuple, Dict

import json
import logging
import pandas as pd

class ETPreprocess:
	def __init__(self, fields_queue: Queue, points_ref: any) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
		self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time'])
 
	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
	
	def start(self, *, request_args: list[ETArg], frequency:str="monthly", logger: logging.Logger = None) -> int:
		'''Begins gathering ET data from listed arguments. Frequency is monthly by default. Returns number of failed rows.'''
		failed_fields = 0
		while self.fields_queue.is_empty() is False:
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			current_crop = self.points_ref['CROP_2020'][current_field_id]
   
			# Creates base dict for each request
			results = [{
       			"success": False,
     			"response": ETRequest(),
        		"data": pd.DataFrame(columns=['field_id', 'crop', 'time', item.name])
          	} for item in request_args]
			
			if logger is not None: logger.info(f"Now analyzing field ID {current_field_id}")
			# Conduct request posts
			for index in range(0, len(request_args)):
				req = request_args[index]
				arg = {
					"date_range": req.date_range,
					"interval": frequency,
					"geometry": current_point_coordinates,
					"model": "Ensemble",
					"units": "mm",
					"variable": req.variable,
					"reference_et": "gridMET",
					"file_format": "JSON"
				}
				response = ETRequest(req.endpoint, arg)
				response.send(logger=logger)

				results[index]["success"] = response.success()
				results[index]["response"] = response
			# End conduct request posts
   
			# There is no failed responses
			if False not in [item['success'] for item in results]:
				for res in results:
					# Begin data composition
					# Data returns as a list containing dict{'time': str, '$variable': float}
					content: List[Dict] = json.loads(res['response'].response.content.decode('utf-8'))
					# item: {'time': str, '$variable': float}
					for item in content:
						res['data'] = pd.concat(
							[pd.DataFrame(
								[[current_field_id, current_crop, item['time'], item[list(item.keys())[1]] ]],
								columns=res['data'].columns
							), res['data']], ignore_index=True
						)
					# End data composition

				if logger is not None: logger.info("Successful")
			else:
				if logger is not None: logger.warning(f"Analyzing for {current_field_id} failed")
				failed_fields+=1
	
			self.fields_queue.dequeue()
			if logger is not None: logger.info(f"{str(self.fields_queue.size())} fields remaining")

		return failed_fields