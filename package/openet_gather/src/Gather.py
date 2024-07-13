from Request import Request
from Arg import Arg
from openet_gather.util.Queue import Queue
from typing import Union, List, Tuple, Dict

import json
import logging
import pandas as pd

class Gather:
	def __init__(self, fields_queue: Queue, points_ref: any, *, api_key: str) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
		self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time'])
		self.__api_key__ = api_key
  
	def __merge__(self, *, tables):
		for table in tables:
			# Conducts full outer joins to preserve time column not always overlapping.
			self.data_table = self.data_table.merge(table, on=['field_id', 'crop', 'time'], how='outer')
   
	def export(self, filename, file_format:str = 'csv', **kwargs) -> None:
		'''Exports data in provided file format. CSV by default. Passes kwargs to matching pandas export function.'''
		match file_format:
			case 'csv':
				self.data_table.to_csv(filename, **kwargs)
			case 'pickle':
				self.data_table.to_pickle(filename, **kwargs)
			case 'json':
				self.data_table.to_json(filename, **kwargs)
			case _:
				raise ValueError(f'Provided file_format "{file_format}" is not supported.')

	def set_api_key(self, api_key: str) -> None:
		self.__api_key__ = api_key

	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
	
	def start(self, *, request_args: list[Arg], frequency:str="monthly", logger: logging.Logger = None) -> int:
		'''Begins gathering ET data from listed arguments.\nFrequency is monthly by default.\nGenerates DataFrame using name of ETArgs as column names.\nReturns number of failed rows.'''
		failed_fields = 0
		tables = [pd.DataFrame(columns=['field_id', 'crop', 'time', item.name]) for item in request_args]

		while self.fields_queue.is_empty() is False:
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			current_crop = self.points_ref['CROP_2020'][current_field_id]
   
			# Creates container to track each request to be made.
			results: List[Request] = [Request() for item in request_args]
			
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
				response = Request(req.endpoint, arg, key=self.__api_key__)
				response.send(logger=logger)

				results[index] = response
			# End conduct request posts
   
			# There is no failed responses
			if False not in [item.success() for item in results]:
				for entry in range(0, len(results)):
					res = results[entry]
					# Begin nth-field data composition
					# Data returns as a list containing dict{'time': str, '$variable': float}
					content: List[Dict] = json.loads(res.response.content.decode('utf-8'))
					# item: {'time': str, '$variable': float}
					for item in content:
						tables[entry] = pd.concat(
							[pd.DataFrame(
								[[current_field_id, current_crop, item['time'], item[list(item.keys())[1]] ]],
								columns=tables[entry].columns
							), tables[entry]], ignore_index=True
						)
					# End nth-field data composition
				if logger is not None: logger.info("Successful")

			else:
				if logger is not None: logger.warning(f"Analyzing for {current_field_id} failed")
				failed_fields+=1

			self.fields_queue.dequeue()
			if logger is not None: logger.info(f"{str(self.fields_queue.size())} fields remaining")

		# Collapses all generates tables into one.
		self.__merge__(tables=tables)
		if logger is not None: logger.info(f"Finished processing. {str(failed_fields)} fields failed.")
		return failed_fields