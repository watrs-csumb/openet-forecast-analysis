from dotenv import dotenv_values

import requests
import time

header = {"Authorization": dotenv_values(".env").get("ET_KEY")}

class ETRequest:
	def __init__(self) -> None:
		pass
	
	def __init__(self, request_endpoint: str ='', request_params: dict ={}, num_retries: int =3) -> None:
		self.request_endpoint = request_endpoint
		self.request_params = request_params
		
		self.send(num_retries)
  
	def set_endpoint(self, request_endpoint: str ='') -> None:
		self.request_endpoint = request_endpoint

	def set_request_params(self, request_params: dict = {}) -> None:
		self.request_params = request_params
		
	def send(self, num_retries: int = 3, cur_retry: int = 1, ignore_fails: bool = False) -> any:
		try:
			self.response = requests.post(
				headers=header,
				url=self.request_endpoint,
				json=self.request_params
			)
			if self.response.status_code != 200:
				raise Exception("[ERROR] Response not valid.")
   
		except:
			while cur_retry <= num_retries:
				print("[WARN] Reattempting request..")
				time.sleep(2 ** cur_retry)
				self.response = self.send(cur_retry=cur_retry+1)
	
		finally:
			if self.response.status_code != 200 and ignore_fails is False:
				reattempt_prompt = input(f"Fetch failed [{str(self.response.status_code)}]: {str(self.response.content)}\nWould you like to reattempt (Y/n)?")
				if reattempt_prompt in ["y", ""]:
					return self.send()
				if reattempt_prompt == "yi":
					return self.send(ignore_fails=True)
			return self.response

	def success(self) -> bool:
		if self.response.status_code != 200:
			return False
		return True


					
		