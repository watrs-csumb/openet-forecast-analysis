
class ETArg:
	def __init__(self, name, *, args: dict) -> None:
		self.name = name
		# Required - no defaults
		self.endpoint = args.get("endpoint", None)
		self.date_range = args.get("date_range", None)
		self.variable = args.get("variable", None)
		# Required - defaults
		self.model = args.get("model", "Ensemble")  # Default model is Ensemble
		self.units = args.get("units", "mm")  # Default units are mm
		self.reference_et = args.get("reference", "gridMET")  # Default reference is gridMET
  
		# Experimental
		self.match_variable = args.get("match_variable", None)
		self.match_window = args.get("match_window", None)

		# Polygon required
		self.reducer = args.get("reducer", None)