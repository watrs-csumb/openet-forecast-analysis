from collections import deque
from datetime import datetime
from ETRequest import ETRequest
from ETArg import ETArg
from pathlib import Path
from typing import List, Dict

import json
import logging
import pandas as pd

class ETFetch:
    """
    OpenET data retrieval configuration. 
    
    Parameters
    ----------
    fields_queue : deque
        Queue of fields that are to be processed. 
            
    points_ref : dict, or DataFrame
        Collection of points that are referenced for the fields. 
        Must contain key or column for USDA Cropland Data Layer and proper WKT formatted coordinates '.geo' column.
        
    api_key : str
        User API key for OpenET API. User restrictions apply.
            
    See Also
    --------
    start : Begin gathering ET data from listed arguments.
    export : Export data in provided file format. CSV by default. Passes kwargs to matching pandas export function.
    ETRequest : ET API Request Handling.
    collections.deque : Thread-safe, memory efficient appends and pops from either side.
    
    Notes
    -----
    Internal DataFrame has default columns ['field_id', 'crop', 'time'] where 'crop' is in reference to crop column in `points_ref`.
    
    Examples
    --------
    Constructing ETFetch using one field
    >>> ref = {'fields': ['CA_062495'], 'CROP_2023': [49], '.geo': [{'type': 'point', 'coordinates': [-121.64489395805282, 36.633390650961346]}]}
    >>> df = pd.DataFrame(data=ref)
    >>> e = ETFetch(fields_queue = deque(df['fields']), points_ref = df, api_key = 'xxxxxx...')
    """
    def __init__(self, fields_queue: deque, points_ref: any, *, api_key: str) -> None:
        self.fields_queue = fields_queue
        self.points_ref = points_ref
        self.data_table = pd.DataFrame(columns=['field_id', 'crop', 'time'])

        # private
        self.__api_key__ = api_key
        self.__names__ = []
        self.__start_time__ = datetime.now()
        self.__timestamp__ = self.__start_time__.strftime('%Y%m%d_%H%M%S')
  
    def __compile_packets__(self) -> None:
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

    def __merge__(self, *, tables) -> None:
        for table in tables:
            # Conducts full outer joins to preserve time column not always overlapping.
            self.data_table = self.data_table.merge(table, on=['field_id', 'crop', 'time'], how='outer')

    def set_api_key(self, api_key: str) -> None:
        self.__api_key__ = api_key

    def set_queue(self, queue: deque) -> None:
        self.fields_queue = queue
        
    def set_reference(self, ref: any) -> None:
        self.points_ref = ref

    def export(self, filename, file_format: str = 'csv', **kwargs) -> None:
        """
        Export data in provided file format. CSV by default. Passes kwargs to matching pandas export function.
        
        Parameters
        ----------
        filename : str, path object, file-like object, or None, default None
            Passed directly into pandas function.
            
        file_format : str, default 'csv'
            File format to be exported. Throws error if not supported.
            
        **kwargs
            Kwargs are passed to matching pandas function.
            
        See Also
        ========
        pd.to_csv : Write object to a comma-separated values (csv) file.
        pd.to_pickle : Pickle (serialize) object to file.
        pd.to_json : Write object to JavaScript Object Notation (JSON) file.
        """
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
           request_args: List[ETArg], 
           frequency: str, 
           packets: bool = True,
           crop_col: str = 'CROP_2023',
           logger: logging.Logger = None) -> int:
        """
        Begin gathering ET data from listed arguments.
          
        Parameters
        ----------
        request_args : Iterable of ETArg
            Iterable container of ETArg that will be used as request parameters for the endpoint specified in ETArg.endpoint.
            If Iterable is a Generator, objects will be consumed as a copy is not made. Use copy() or deepcopy() if Iterable is a Generator.

        frequency : str
            Accepts 'daily' or 'monthly'. Applied to all endpoints in request_args.
        
        packets : bool, default True
            Data is stored in files per ETArg per field and compiled into one file at the end of retrieval.
            If False, all data is stored in memory and compiled into a single pandas DataFrame at the end of retrieval.
            If retrieving large amounts of data, default True is recommended.
            
        crop_col : str, default 'CROP_2023'
            Name of column used to reference USDA's Cropland Data Layer code.
            
        logger : logging.Logger, default None
            If logger is provided, logs request success and failure activity.
            Recommended for debugging.
            
        Returns
        -------
        int
            Number of fields that failed to be retrieved.
        
        See Also
        --------
        ETFetch : Constructor.
        ETArg : Struct-like Class of request arguments with optimized settings.
        
        Notes
        -----
        If packeting is enabled, data is stored in `./data/bin/`.
        
        If a request for a field fails, the entire field is discarded regardless if other requests succeeded.
        
        Examples
        --------
        Start data fetch from constructor
        >>> arg = ETArg(
            "expected_et",
            args={
                "endpoint": "https://developer.openet-api.org/experimental/raster/timeseries/forecasting/seasonal",
                "date_range": ["2016-01-01", "2024-07-01"],
                "variable": "ET",
            },
        )
        >>> ref = {'fields': ['CA_062495'], 'CROP_2023': [49], '.geo': [{'type': 'point', 'coordinates': [-121.64489395805282, 36.633390650961346]}]}
        >>> df = pd.DataFrame(data=ref)
        >>> e = ETFetch(fields_queue = deque(df['fields']), points_ref = df, api_key = 'xxxxxx...')
        >>> e.start(request_args = [arg], frequency = 'monthly')
        """
        failed_fields = 0
        tables = [pd.DataFrame(columns=['field_id', 'crop', 'time', item.name]) for item in request_args]

        while (len(self.fields_queue) == 0) is False:
            current_field_id = self.fields_queue[0]
            current_point_coordinates = json.loads(self.points_ref['.geo'][current_field_id])['coordinates']
            current_crop = self.points_ref[crop_col][current_field_id]
   
            # Creates container to track each request to be made.
            results: List[ETRequest] = [ETRequest() for item in request_args]
            self.__names__ = [item.name for item in request_args]
            
            if logger:
                logger.info(f"Now analyzing field ID {current_field_id}")
            # Conduct request posts
            for index in range(0, len(request_args)):
                req = request_args[index]
                arg = {
                    "geometry": current_point_coordinates,
                    "variable": req.variable,
                    "file_format": "JSON"
                }
                arg['align'] = req.align
                arg['model'] = req.model
                arg['units'] = req.units
                arg['reference_et'] = req.reference
                # Below are optional fields. Included only if they exist
                if req.date_range:
                    arg['date_range'] = req.date_range
                if req.reducer:
                    arg['reducer'] = req.reducer
                if req.match_variable:
                    arg['match_variable'] = req.match_variable
                if req.match_window:
                    arg['match_window'] = req.match_window
     
                if frequency:
                    arg['interval'] = frequency

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

                if logger:
                    logger.info("Successful")

            else:
                if logger:
                    logger.warning(f"Analyzing for {current_field_id} failed")
                failed_fields+=1

            self.fields_queue.popleft()
            if logger:
                logger.info(f"{str(len(self.fields_queue))} fields remaining")

        # Produces data table depending on if this process enabled packets.
        if packets:
            self.__compile_packets__()
        else:
            self.__merge__(tables=tables)

        if logger:
            self.__end_time__ = datetime.now()
            time_elapsed = (self.__end_time__ - self.__start_time__)
            logger.info(f"Finished processing. {str(failed_fields)} fields failed. Elapsed time: {str(time_elapsed)}")
        return failed_fields