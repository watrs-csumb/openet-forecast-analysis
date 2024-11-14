from google.cloud import storage
from logging import Logger

import pandas as pd

class ETCloudStorage:
    def __init__(self, project_id, client_key=None, logger=None):
        self.__project_id__ = project_id
        self.__client__ = storage.Client(project=self.__project_id__, credentials=client_key)
        self.__logger__: None|Logger = logger
        
    @property
    def client(self) -> storage.Client:
        return self.__client__
    
    @property
    def project_id(self) -> str:
        return self.__project_id__
    
    @property
    def bucket(self) -> storage.Bucket:
        return self.client.get_bucket("forecasting-temp")
        
    def pd_write(self, blob_name: str, data: any, **kwargs) -> None:
        # Creates GCS object
        blob = self.bucket.blob(blob_name)
    
        with blob.open('w') as file:
            if isinstance(data, pd.DataFrame):
                file.write(data.to_csv(**kwargs))
                
            file.write(data)
            if self.__logger__:
                self.__logger__.info(f'Wrote {blob.size} bytes to {blob.name} in {self.bucket.name}')

    def pd_read(self, blob_name: str, **kwargs) -> pd.DataFrame:
        blob = self.bucket.blob(blob_name)
        
        with blob.open('r') as file:
            return pd.read_csv(file, **kwargs)