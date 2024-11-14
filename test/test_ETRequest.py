from src.ETRequest import ETRequest, status_whitelist

def ETRequest_successful(requests_mock):
    res = ETRequest(
        request_endpoint='https://developer.openet.org/awesome_endpoint',
        request_params={},
        key='1234567890'
    )
    
    requests_mock.post(res.request_endpoint, status_code=status_whitelist[0])
    
    res.send()
    
    assert res.success() is True # Making it explicit for sanity check

def ETRequest_unsuccessful(requests_mock, monkeypatch):
    res = ETRequest(
        request_endpoint="https://developer.openet.org/awesome_endpoint",
        request_params={},
        key="1234567890",
    )
    
    requests_mock.post(res.request_endpoint, status_code=403)
    monkeypatch.setattr('builtins.input', lambda _: 'n')
    
    res.send()
    
    assert res.success() is False

def ETRequest_retry_successful(requests_mock):
    res = ETRequest(
        request_endpoint="https://developer.openet.org/awesome_endpoint",
        request_params={},
        key="1234567890",
    )
    
    requests_mock.post(res.request_endpoint, [{'status_code': 403}, {'status_code': 200}])
    
    res.send()
    
    assert res._current_attempt == 1    # Initial value is 0.
    assert res.success() is True        # Explicit for sanity check.
    
def ETRequest_prompt_successful(requests_mock, monkeypatch):
    res = ETRequest(
        request_endpoint="https://developer.openet.org/awesome_endpoint",
        request_params={},
        key="1234567890",
    )

    requests_mock.post(res.request_endpoint, [ {"status_code": 403}, {"status_code": 403}, {"status_code": 403},{"status_code": 200}])
    monkeypatch.setattr('builtins.input', lambda _: 'y')
    
    res.send()
    
    assert res.success() is True