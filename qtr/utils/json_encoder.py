from typing import Any
import json
import sympy
from datetime import datetime
from qtr.base.nonjsonable import NoneJsonable
from qtr.base.jsonable import Jsonable
from qtr.utils.constants import TradingConstants

class TradingObjectEncode(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Jsonable):
            obj = o.__dict__.copy()
            return obj
        elif isinstance(o, sympy.core.numbers.Float):
            return float(o)
        elif isinstance(o, datetime):
            return datetime.strftime(o, TradingConstants.TIME_FORMAT)
        elif isinstance(o, NoneJsonable):
            return None
        return super().default(o)