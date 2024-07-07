
class ETArg:
	def __init__(self, args: dict) -> None:
		self._endpoint  = args["endpoint"]
		self._date_range = args["date_range"]
		self._variable = args["ET"]
	
	@property
	def date_range(self):
		return self._date_range

	@date_range.setter
	def date_range(self, range: list):
		self._date_range = range
  
	@property
	def endpoint(self):
		return self._endpoint

	@endpoint.setter
	def endpoint(self, endpoint: str):
		self._endpoint = endpoint
  
	@property
	def variable(self):
		return self._variable

	@variable.setter
	def variable(self, variable: str):
		self._variable = variable