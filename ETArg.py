
class ETArg:
    def __init__(self, name, *, args: dict) -> None:
        self._name = name
        self._endpoint = args.get("endpoint", None)
        self._date_range = args.get("date_range", None)
        self._variable = args.get("variable", None)

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