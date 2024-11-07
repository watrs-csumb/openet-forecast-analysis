from ETArg import ETArg
from ETFetch import ETFetch
from ETRequest import ETRequest
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
    endpoint = 'https://developer.openet.org/awesome_endpoint',
    et_arg = ETArg(
        "et",
        args={
            "endpoint": endpoint,
            "date_range": ["2023-06-01", "2023-07-01"],
            "variable": "ET",
        },
    )
    
    queue = deque(['CA_0', 'CA_1', 'CA_2'])
    
    reference = pd.read_csv('./test/mock_fields.csv').set_index('OPENET_ID')
    
    yield et_arg, queue, reference
    # Clean up conducted bottom-up
    # if Path('./data/').exists():
    #     rmtree('./data/')

def ETFetch_good(requests_mock, setup):
    pytest.skip("Unfinished test.")
    et_arg, queue, reference = setup
    
    # Have the request respond with following data:
    requests_mock.register_uri('POST', rm.ANY)      # Bug: request_mock modifies the uri string at some point which causing the matching to fail. 
    requests_mock.post(
        et_arg.endpoint,
        [
            {"status_code": 200, "content": b"[{'time': '2023-06-01', 'et': 0.12}]"},
            {"status_code": 200, "content": b"[{'time': '2023-06-01', 'et': 0.15}]"},
            {"status_code": 200, "content": b"[{'time': '2023-06-01', 'et': 0.13}]"},
        ],
    )
    
    fetch = ETFetch(deepcopy(queue), reference, api_key='1234567890')
    tmp_path = f"./data/bin/{fetch.__timestamp__}"
    
    fetch.start(request_args=[et_arg], frequency='monthly', packets=True)
    
    # .start() will create a data folder at this level if it doesn't exist
    assert Path(tmp_path).exists() is True                  # explicit for sanity check
    assert len(list(Path.glob(f'{tmp_path}/*.csv'))) == 3   # temp folder should contain 3 files. one for each field.
    
    result_data = pd.DataFrame([['CA_0', 49, '06-01-2023', 0.12],
                                ['CA_1', 61, '06-01-2023', 0.15],
                                ['CA_2', 49, '06-01-2023', 0.13]], columns=['field_id', 'crop', 'time', 'et'])
    
    pd_testing.assert_frame_equal(fetch.data_table, result_data, check_like=True) # Ignore order.
