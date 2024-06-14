from dotenv import dotenv_values

import requests
import time

header = {"Authorization": dotenv_values(".env").get("ET_KEY")}

class ETRequest:
	def __init__(self):
		pass
	
	def __init__(self, request_endpoint='', request_params={}, num_retries=3):
		self.request_endpoint = request_endpoint
		self.request_params = request_params
		
		self.send(num_retries)
		
	def send(self, num_retries=3, cur_retry=1):
		ignore_fails = False
		try:
			self.response = requests.post(
				headers=header,
				url=self.request_endpoint,
				json=self.request_params
			)
   
		except requests.exceptions.ConnectionError:
			while cur_retry < num_retries:
				time.sleep(2 ** cur_retry)
				print('reattempting..')
				self.response = self.send(num_retries, cur_retry=cur_retry+1)
	
		finally:
			if self.response.status_code not in [200] and ignore_fails is not True:
				reattempt_prompt = input("Fetch for " + " failed (" + str(self.response.status_code) + "): " + str(self.response.content) + "\nWould you like to reattempt (Y/n)?")
				if reattempt_prompt in ["y", ""]:
					return self.send()
			return self.response

	def success(self):
		if self.response.status_code not in [200]:
			print('failed.. did it ask??')
			return False
		print('success!')
		return True


					
		