from datetime import datetime
from ETRequest import ETRequest
from ETArg import ETArg
from pathlib import Path
from Queue import Queue
from typing import Union, List, Tuple, Dict

import json
import logging
import pandas as pd

class ETPreprocess:
	def __init__(self, fields_queue: Queue, points_ref: any, *, api_key: str) -> None:
		self.fields_queue = fields_queue
		self.points_ref = points_ref
		self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time'])

		# private
		self.__api_key__ = api_key
		self.__names__ = []
		self.__timestamp__ = datetime.now().strftime('%Y%m%d_%H%M%S')
  
	def __merge__(self, *, tables) -> None:
		for table in tables:
			# Conducts full outer joins to preserve time column not always overlapping.
			self.data_table = self.data_table.merge(table, on=['field_id', 'crop', 'time'], how='outer')

	def set_api_key(self, api_key: str) -> None:
		self.__api_key__ = api_key

	def set_queue(self, queue: Queue) -> None:
		self.fields_queue = queue
		
	def set_reference(self, ref: any) -> None:
		self.points_ref = ref
  
	def compile_packets(self) -> None:
		# Create empty tables for each column name. Will all be merged at the end.
		tables = [pd.DataFrame(columns=['field_id', 'crop', 'time', name]) for name in self.__names__]
		# Iterate through each column name first
		for item in range(0, len(self.__names__)):
			name = self.__names__[item]
			# Collect list of files whose name contains the current column name
			files = Path(f'data/bin/{self.__timestamp__}/').glob(f'*.{name}.csv')

			# Iterate through each file through Generator iterator
			for file in files:
				# e.g. CA_270812.27.actual_eto.csv
				# becomes ['CA_270812', '27', 'actual_eto', 'csv']
				parts = str(file.name).split('.')
				# Contains [time, {variable}]
				data = pd.read_csv(file, header=0, names=['time', name])
				data['field_id'] = parts[0]
				data['crop'] = parts[1]
				tables[item] = pd.concat([data, tables[item]], ignore_index=True)

		self.__merge__(tables=tables)

	def export(self, filename, file_format: str = 'csv', **kwargs) -> None:
		'''Exports data in provided file format. CSV by default. Passes kwargs to matching pandas export function.'''
		match file_format:
			case 'csv':
				self.data_table.to_csv(filename, index=False, **kwargs)
			case 'pickle':
				self.data_table.to_pickle(filename, index=False, **kwargs)
			case 'json':
				self.data_table.to_json(filename, index=False, **kwargs)
			case _:
				raise ValueError(f'Provided file_format "{file_format}" is not supported.')
	
	def start(self, *, 
           request_args: list[ETArg], 
           frequency: str = "monthly", 
           logger: logging.Logger = None,
           packets: bool = False,
           crop_col: str = 'CROP_2023') -> int:
		'''Begins gathering ET data from listed arguments.\nFrequency is monthly by default.\nGenerates DataFrame using name of ETArgs as column names.\nReturns number of failed rows.'''
		failed_fields = 0
		tables = [pd.DataFrame(columns=['field_id', 'crop', 'time', item.name]) for item in request_args]

		while self.fields_queue.is_empty() is False:
			current_field_id = self.fields_queue.front()
			current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
			current_crop = self.points_ref[crop_col][current_field_id]
   
			# Creates container to track each request to be made.
			results: List[ETRequest] = [ETRequest() for item in request_args]
			self.__names__ = [item.name for item in request_args]
			
			if logger: logger.info(f"Now analyzing field ID {current_field_id}")
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
				response = ETRequest(req.endpoint, arg, key=self.__api_key__)
				response.send(logger=logger)

				results[index] = response
			# End conduct request posts
   
			# There is no failed responses
			if False not in [item.success() for item in results]:
				for entry in range(0, len(results)):
					res = results[entry]
					name = request_args[entry].name
     				# Data returns as a list containing dict{'time': str, '$variable': float}
					content: List[Dict] = json.loads(res.response.content.decode('utf-8'))

					# Begin nth-field data composition
					if packets:
						# Path used for data dumping uses timestamp of initial program run.
						path = Path(f'data/bin/{self.__timestamp__}')
						# Check if data bin exists, if not then create it
						if path.exists() is False:
							path.mkdir(parents=True)
						# Converts decoded JSON string to DataFrame, then exports as csv file
						# Filename e.g. CA_270812.27.actual_eto.csv
						pd.json_normalize(content).to_csv(f'{path}/{current_field_id}.{current_crop}.{name}.csv', index=False)
					else:
						# item: {'time': str, '$variable': float}
						for item in content:
							tables[entry] = pd.concat(
								[pd.DataFrame(
									[[current_field_id, current_crop, item['time'], item[list(item.keys())[1]]]],
									columns=tables[entry].columns), tables[entry]], ignore_index=True)
					# End nth-field data composition

				if logger: logger.info("Successful")

			else:
				if logger: logger.warning(f"Analyzing for {current_field_id} failed")
				failed_fields+=1

			self.fields_queue.dequeue()
			if logger: logger.info(f"{str(self.fields_queue.size())} fields remaining")

		# Produces data table depending on if this process enabled packets.
		if packets:
			self.compile_packets()
		else:
			self.__merge__(tables=tables)

		if logger: logger.info(f"Finished processing. {str(failed_fields)} fields failed.")
		return failed_fields