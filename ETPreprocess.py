from ETRequest import ETRequest
from ETArg import ETArg
from Queue import Queue
from typing import Union, List

import json
import logging
import pandas as pd

class ETPreprocess:
	def __init__(self, fields_queue: Queue, points_ref: any) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
	
	def set_table(self, *, columns: list = ['']) -> pd.DataFrame:
		'''Sets the output table's columns. columns should be a list of strings and in the order that requests are to be made.
  			Returns DataFrame'''
		self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time'] + columns)
		return
 
	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
	
	def start(self, *, request_args: list[ETArg], frequency:str="monthly", logger: logging.Logger = None) -> int:
		'''Begins gathering ET data from listed arguments. Frequency is monthly by default. Returns number of failed rows.'''
		# Fails if set_table has not been called.
		if self.data_table is None:
			raise ReferenceError('data_table not set. Call set_table() to specify columns.')

		failed_fields = 0
		while self.fields_queue.is_empty() is False:
			# Create list of bools and {} of size of argument list. Default False
			# The empty object contains the response for corresponding request
			successes: List[List[Union[bool, ETRequest]]] = [[False, ETRequest()] for item in len(request_args)]
   
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			current_crop = self.points_ref['CROP_2020'][current_field_id]
			
			if logger is not None: logger.info(f"Now analyzing field ID {current_field_id}")
			
			# Loop through all arguments
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
				successes[index] = [response.success(), response]
   
			# There is no failed responses.
			if False not in successes:
				# Data returns as a list[12] for each
				# Each item is of dict{'time': str, 'et': float}
				data_array = []
				for res in successes:
					content: str = json.loads(res[1].response.content.decode('utf-8'))
					# Resulting shape should be n-requests x (n-fields * n-dates)
					data_array.append([data for data in content])
 
				# for item in timeseries_data:
				# 	self.data_table = pd.concat(
         		# 		[pd.DataFrame(
                #  			[[current_field_id, current_crop, entry_time, entry_et_actual, entry_et_forecast]],
                #     		columns=self.data_table.columns),
              	# 			self.data_table],
             	# 		ignore_index=True)
	 
				if logger is not None: logger.info("Successful")
			else:
				if logger is not None: logger.warning(f"Analyzing for {current_field_id} failed")
				failed_fields+=1
	
			self.fields_queue.dequeue()
			if logger is not None: logger.info(f"{str(self.fields_queue.size())} fields remaining")
		
  		# Wierdly enough, presetting the index will cause a failure. So set index after completion.
		self.data_table.set_index('field_id', inplace=True)
		return failed_fields