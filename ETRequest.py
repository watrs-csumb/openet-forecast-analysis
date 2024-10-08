import logging
import requests
import time

# ONLY CHANGE IF YOU KNOW WHAT YOU ARE DOING
status_whitelist = [200]

class ETRequest:
	def __init__(
		self, request_endpoint: str = "", request_params: dict = {}, key: str = ""
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
		"""Sends POST request and returns response. Will make num_retries reattempts if it fails.
		If failures are meant to be ignored, set ignore_fails to True. It is not recommended to modify cur_retry"""
		try:
			self.response = requests.post(
				headers=self.header, url=self.request_endpoint, json=self.request_params
			)

			# Only a 200 Response will return the right data
			if ignore_fails is False and self.success() is False:
				raise ValueError

		# Allow keyboard interruption
		except KeyboardInterrupt:
			pass

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
					prompt_info = ". Please check your connection."

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
					self.response = None

			return self.response

	def success(self) -> bool:
		"""Returns boolean depending on if the request succeeded or not"""
		try:
			return self.response.status_code in status_whitelist
		except:
			return False


					
		