import logging
import requests
import time

# ONLY CHANGE IF YOU KNOW WHAT YOU ARE DOING
status_whitelist = [200]

class ETRequest:
    """
    ET API Request Handling.
    
    Parameters
    ----------
    request_endpoint : str
        OpenET API endpoint

    request_params : dict
        Dict of request parameters for API endpoint

    key : str
        User API key for OpenET API. User restrictions apply.
    
    Notes
    -----
    None provided.
    
    Examples
    --------
    None provided.
    """
    def __init__(
        self, request_endpoint: str = '', request_params: dict = {}, key: str = ""
    ) -> None:
        """Creates ETRequest object"""
        self.request_endpoint = request_endpoint
        self.request_params = request_params
        self.header = {"Authorization": key}
        self._current_attempt = 0

    def set_api_key(self, key: str) -> None:
        self.header = {"Authorization": key}

    def set_endpoint(self, request_endpoint: str = "") -> None:
        self.request_endpoint = request_endpoint

    def set_request_params(self, request_params: dict = {}) -> None:
        self.request_params = request_params

    def send(
        self,
        *,
        num_retries: int = 3,
        ignore_fails: bool = False,
        logger: logging.Logger = None,
    ) -> any:
        """
        Send POST request and returns response.
        
        Parameters
        ----------
        num_retries : int, default 3
            Number of times to retry if request fails.
        
        ignore_fails : bool, default False
            Whether to ignore failures. If True, failures will return None and not reattempt.
        
        logger : logging.Logger, default None
            If logger is provided, logs request success and failure activity.
            Recommended for debugging.
            
        Returns
        -------
        requests.Response, or None
            `Response <Response` object or `None`.
        
        Notes
        -----
        * Success is determined if status code is contained within a whitelist, containing only 200 (Success).
        * Request can be keyboard interrupted. Will return `None` immediately.
        * Reattempts are done in 2^(1, ...n) second increments
        * On failure, reattempt prompt is shown. 
            * 'yi' will reattempt and ignore future failures for current request.
            * Thread will hang until there is a response.
            
        Examples
        --------
        Send request to raster timeseries endpoint.
        >>> endpoint = "https://developer.openet-api.org/raster/timeseries/point"
        >>> args = {
                "date_range": [
                    "2020-01-01",
                    "2020-12-31"
                ],
                "file_format": "CSV",
                "geometry": [
                    -121.36322,
                    38.87626
                ],
                "interval": "monthly",
                "model": "Ensemble",
                "reference_et": "gridMET",
                "units": "mm",
                "variable": "ET"
            }
        >>> req = ETRequest(endpoint, args, 'xxx...')
        >>> res = req.send() # Uses default parameters.
        >>> res.content
        time,et
        '2020-01-01',27.0
        '2020-02-01',58.0
        '2020-03-01',70.0
        '2020-04-01',110.0
        '2020-05-01',69.0
        '2020-06-01',25.0
        '2020-07-01',13.0
        '2020-08-01',22.0
        '2020-09-01',22.0
        '2020-10-01',7.0
        '2020-11-01',17.0
        '2020-12-01',21.0
        """
        try:
            self.response = requests.post(
                headers=self.header, url=self.request_endpoint, json=self.request_params
            )

            # Only a 200 Response will return the right data
            if ignore_fails is False and self.success() is False:
                raise ValueError
            # Only reachable if ignore_fails is True
            return self.response

        # Allow keyboard interruption
        except KeyboardInterrupt:
            num_retries = 0
            return None

        except Exception:
            while self._current_attempt < num_retries and self.success() is False:
                if logger is not None:
                    logger.warning("Reattempting request..")

                time.sleep(2**self._current_attempt)
                self._current_attempt += 1
                self.response = self.send(logger=logger)

        finally:
            # This branch will only happen if all reattempts failed. Likely due to an outage.
            if self.success() is False and ignore_fails is False:
                # Attempt to build a detailed prompt.
                # Shows status_code and message if properties exist on response. Otherwise, no additional details.
                # A detailed message would not be provided in the event of an outage.
                prompt_info = ""
                try:
                    prompt_info = f"[{self.response.status_code}]: {self.response.content}"
                except Exception:
                    prompt_info = "No response. Please check your connection."
                if logger is not None:
                    logger.error(prompt_info)
                    logger.info(self.request_params)
                reattempt_prompt = input(
                    f"Fetch failed{prompt_info}\nWould you like to reattempt (Y/n)? "
                )
                if reattempt_prompt.lower() in ["y", ""]:
                    return self.send(logger=logger)
                # In this case, yi means "yes and ignore"
                elif reattempt_prompt.lower() == "yi":
                    return self.send(logger=logger, ignore_fails=True)
                else:
                    num_retries = 0
                    self.response = None

            return self.response

    def success(self) -> bool:
        """
        Whether request succeeded.
        
        Returns
        -------
        bool
            Returns True if status_code is in status_whitelist.
            
        Notes
        -----
        * The status_whitelist only contains 200 for request success.
        """
        try:
            return self.response.status_code in status_whitelist
        except Exception:
            return False