import time
import warnings

from logging import Logger, WARNING, ERROR, addLevelName
from requests import Response, post
from requests.exceptions import Timeout, ConnectionError

from .ETException import ETException

STATUS_ALLOWED = [200]
TIMEOUT = 60 * 5

HELPFUL = 25
addLevelName(HELPFUL, "HELPFUL")

class Request:
    def __init__(
        self, 
        endpoint: str | None = None, 
        params: dict | None = {}, 
        key: str | None = None, 
        logger: Logger | None = None
    ) -> None:
        self.endpoint = endpoint
        self.params = params
        self.header = {"Authorization": key}
        self.logger = logger

        self._attempt: int = 1
        self.response: Response | None = None

    def _retry_request(self, n_retries: int = 3) -> None:
        if not self.endpoint:
            raise AttributeError("No endpoint provided for request.")
        if not self.params:
            raise AttributeError("No parameters provided for request.")
        if isinstance(self.params, dict) and len(self.params.keys()) == 0:
            raise AttributeError("Request parameters cannot be empty.")
        if not self.header.get("Authorization", None):
            raise AttributeError("No Authorization key provided for request.")
        
        res = None
        err = None
        
        for attempt in range(self._attempt, n_retries + 1):
            if attempt > 1:
                # Exponential backoff
                time.sleep(min(2 ** (attempt - 1), 60))
            try:
                res = post(
                    url=self.endpoint,
                    json=self.params,
                    headers=self.header,
                    timeout=TIMEOUT
                )
                
                if not self.success(res):
                    raise ValueError()
                
            except (Timeout, ConnectionError, ValueError) as e:
                err = e
            
            if err is not None:
                if attempt == n_retries:
                    raise ETException
                
                if self.logger:
                    self.logger.log(
                        ERROR,
                        f"Attempt {attempt} failed for endpoint {self.endpoint}: {err}"
                    )
                continue
        
        self.response = res
    
    def send(self, n_retries: int = 3) -> Response | None:
        try:
            self._retry_request(n_retries)
        except ETException:
            return None
        
        return self.response

    def success(self, request: Response | None = None) -> bool:
        # Returns true in the event that a response is returned and its status code is in STATUS_ALLOWED.
        req = request or self.response
        
        try:
            return req.status_code in STATUS_ALLOWED  # type: ignore
        except Exception:
            return False

class ETRequest(Request):
    def __init__(self, request_endpoint=None, request_params=None, key=None) -> None:
        warnings.warn(
            "ETRequest is deprecated. Use Request instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(endpoint=request_endpoint, params=request_params, key=key)
        
    def send(self, logger=None, *args, **kwargs):
        super().send(*args, **kwargs)
