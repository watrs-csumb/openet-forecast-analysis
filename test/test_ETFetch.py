from src import ETFetch, ETArg
from pathlib import Path
from shutil import rmtree

from collections import deque
from copy import deepcopy

import pandas as pd
import pandas.testing as pd_testing
import pytest
import requests_mock as rm

@pytest.fixture
def setup(): 
    queue = deque(['CA_0', 'CA_1', 'CA_2'])
    
    reference = pd.read_csv('./test/mock_fields.csv').set_index('OPENET_ID')
    
    yield queue, reference

def ETFetch_good(requests_mock: rm.Mocker, setup):
    queue, reference = setup
    et_arg = ETArg(
        "et",
        args={
            "endpoint": "https://developer.openet.org/awesome_endpoint",
            "date_range": ["2023-06-01", "2023-07-01"],
            "variable": "ET",
        },
    )
    
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

    result_data = pd.read_csv("./test/mock_result.csv")
    
    # .start() will create a data folder at this level if it doesn't exist
    assert Path(tmp_path).exists() is True                  # explicit for sanity check
    assert len(list(Path(tmp_path).glob('*.csv'))) == 3     # temp folder should contain 3 files. one for each field.
    
    pd_testing.assert_frame_equal(fetch.data_table, result_data, check_like=True, check_dtype=False) # Ignore order.
    
    rmtree(tmp_path)
    assert Path(tmp_path).exists() is False                 # Verify cleanup
