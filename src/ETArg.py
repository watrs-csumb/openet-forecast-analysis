
class ETArg:
    def __init__(self, name, *, args: dict) -> None:
        self._name = name
        # Required - no defaults
        self._endpoint = args.get("endpoint", None)
        self._date_range = args.get("date_range", None)
        self._variable = args.get("variable", None)
        # Required - defaults
        self._align = args.get('align', False) # Default no alignment
        self._model = args.get("model", "Ensemble")  # Default model is Ensemble
        self._units = args.get("units", "mm")  # Default units are mm
        self._reference = args.get("reference", "gridMET")  # Default reference is gridMET

        # Experimental
        self._match_variable = args.get("match_variable", None)
        self._match_window = args.get("match_window", None)
        self._cog = args.get("cog", None)
        self._encrypt = args.get("encrypt", None)

        # Polygon required
        self._reducer = args.get("reducer", None)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def date_range(self) -> list[str]:
        return self._date_range

    @date_range.setter
    def date_range(self, range: list):
        self._date_range = range

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @endpoint.setter
    def endpoint(self, endpoint: str):
        self._endpoint = endpoint

    @property
    def variable(self) -> str:
        return self._variable

    @variable.setter
    def variable(self, variable: str):
        self._variable = variable

    @property
    def cog(self):
        return self._cog
    
    @cog.setter
    def cog(self, optimize: bool):
        self._cog = optimize
        
    @property
    def encrypt(self):
        return self._encrypt
    
    @encrypt.setter
    def encrypt(self, do_encrypt: bool):
        self._encrypt = do_encrypt

    @property
    def align(self):
        return self._align

    @align.setter
    def align(self, do_align):
        self._align = do_align

    @property
    def model(self) -> str:
        return self._model

    @model.setter
    def model(self, model: str):
        self._model = model

    @property
    def units(self) -> str:
        return self._units

    @units.setter
    def units(self, unit: str):
        self._units = unit
    
    @property
    def reference(self)->str:
        return self._reference

    @reference.setter
    def reference(self, reference: str):
        self._reference = reference

    @property
    def match_variable(self):
        return self._match_variable

    @match_variable.setter
    def match_variable(self, variable):
        self._match_variable = variable
  
    @property
    def match_window(self):
        return self._match_window

    @match_window.setter
    def match_window(self, window):
        self._match_window = window

    @property
    def reducer(self) -> str:
        return self._reducer

    @reducer.setter
    def reducer(self, method: str):
        self._reducer = method