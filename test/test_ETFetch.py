from src import ETFetch, ETArg

from collections import deque
from copy import deepcopy
from io import BytesIO
from pathlib import Path

from google.cloud.storage import Blob

import logging
import pandas as pd
import pandas.testing as pd_testing
import pytest
import requests_mock as rm

class Test_ETFetch:
    @pytest.fixture
    def setup(self, cleandir): 
        queue = deque(['CA_0', 'CA_1', 'CA_2'])
        cwd = cleandir
        reference = pd.read_csv(f'{cwd}/test/mock_fields.csv').set_index('OPENET_ID')
        
        et_arg = ETArg(
            "et",
            args={
                "endpoint": "https://developer.openet.org/awesome_endpoint",
                "date_range": ["2023-06-01", "2023-07-01"],
                "variable": "ET",
            },
        )
        
        yield queue, reference, et_arg
    
    def ETFetch_good(self, requests_mock: rm.Mocker, setup, cleandir):
        queue, reference, et_arg = setup
        cwd = cleandir
        
        requests_mock.post(
            url="https://developer.openet.org/awesome_endpoint", response_list=
            [
                {"status_code": 200, "content": b'[{"time": "2023-06-01", "et": 0.12}]'},
                {"status_code": 200, "content": b'[{"time": "2023-06-01", "et": 0.15}]'},
                {"status_code": 200, "content": b'[{"time": "2023-06-01", "et": 0.13}]'},
            ],
        )
        
        fetch = ETFetch(deepcopy(queue), reference, api_key='1234567890')
        tmp_path = f"./data/bin/{fetch.__timestamp__}"
        
        fetch.start(request_args=[et_arg], frequency='monthly', packets=True)

        result_data = pd.read_csv(f"{cwd}/test/mock_result.csv")
        
        # .start() will create a data folder at this level if it doesn't exist
        assert Path(tmp_path).exists() is True                  # explicit for sanity check
        assert len(list(Path(tmp_path).glob('*.csv'))) == 3     # temp folder should contain 3 files. one for each field.
        
        pd_testing.assert_frame_equal(fetch.data_table, result_data, check_like=True, check_dtype=False) # Ignore order.

    @pytest.mark.skip(reason="Requests mock is producing inconsistent behavior. Needs investigating.")
    def ETFetch_missing_field(self, monkeypatch, requests_mock, setup):
        queue, reference, et_arg = setup
        
        requests_mock.post(
            url="https://developer.openet.org/awesome_endpoint",
            response_list=[
                {"status_code": 200, "content": b'[{"time": "2023-06-01", "et": 0.12}]'},
                {"status_code": 200, "content": b'[{"time": "2023-06-01", "et": 0.13}]'},
                {"status_code": 403},
            ],
        )
        monkeypatch.setattr("builtins.input", lambda _: "n")
        
        fetch = ETFetch(deepcopy(queue), reference, api_key="1234567890")
        tmp_path = f"./data/bin/{fetch.__timestamp__}"
        fetch.start(request_args=[et_arg], frequency="monthly", packets=True)
        
        assert Path(tmp_path).exists() is True  # explicit for sanity check
        # assert (len(list(Path(tmp_path).glob("*.csv"))) == 2)  # temp folder should contain 2 files. one for each field.

        # After one failure, there should only be 2 fields instead of three.
        assert len(fetch.data_table) == 2 

    @pytest.mark.skip(reason="Unfinished test. Need to upload.")
    def ETFetch_good_with_gcp_export(self, setup, gcp_server_mock, cleandir):
        queue, reference, et_arg = setup
        server, client = gcp_server_mock
        cwd = cleandir
        
        with rm.Mocker() as request_mock:
            request_mock.post(
                url="https://developer.openet.org/awesome_endpoint",
                response_list=[
                    {
                        "status_code": 200,
                        "content": b'[{"time": "2023-06-01", "et": 0.12}]',
                    },
                    {
                        "status_code": 200,
                        "content": b'[{"time": "2023-06-01", "et": 0.15}]',
                    },
                    {
                        "status_code": 200,
                        "content": b'[{"time": "2023-06-01", "et": 0.13}]',
                    },
                ],
            )
            fetch = ETFetch(deepcopy(queue), reference, api_key="1234567890")
            fetch.start(request_args=[et_arg], frequency="monthly")
        
        expected_result = pd.read_csv(f"{cwd}/test/mock_result.csv")
        
        blob: Blob = client.fetch_save(fetch, "ETFetch_good_with_gcp_export.csv", parents=True)
        
        blob.reload()
        
        assert blob.name == "ETFetch_good_with_gcp_export.csv"
        
        blob_file = BytesIO()
        blob.download_as_bytes(blob_file)
        
        with open(expected_result, "rb") as result_reader:
            assert blob_file.getvalue() == result_reader.read()
