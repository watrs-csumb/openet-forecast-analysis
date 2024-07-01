
class ETArg:
	def __init__(self, args: dict) -> None:
		self._endpoint  = args["endpoint"]
		self._date_range = args["date_range"]
		self._interval = args["interval"],
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
	def interval(self):
		return self._interval

	@interval.setter
	def interval(self, interval: str):
		self._interval = interval
  
	@property
	def variable(self):
		return self._variable

	@variable.setter
	def variable(self, variable: str):
		self._variable = variable