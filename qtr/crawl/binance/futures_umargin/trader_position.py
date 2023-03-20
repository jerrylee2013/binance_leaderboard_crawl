from sys import getsizeof
from typing_extensions import Self

from qtr.base.jsonable import Jsonable
from qtr.utils.constants import TradingConstants
class Formatter():
    MESSAGE_LENGTH = 1800

    def utf8len(s: str):
        return getsizeof(s)

class TraderPosition(Jsonable):
    def make_position_list(raw_list: list[dict]):
        new_position_list = []
        for p in raw_list:
            pos = TraderPosition(symbol=p.get("symbol"), leverage=p.get("leverage"),
                                 amount=p.get("amount"), entry_price=p.get("entryPrice"),
                                 update_time=p.get("updateTimeStamp"), mark_price=p.get("markPrice"),
                                pnl=p.get("pnl"), roe=p.get("roe"), yellow=p.get("yellow"),
                                trade_before=p.get("tradeBefore"))
            new_position_list.append(pos)
        return new_position_list

    def __init__(self, symbol: str, leverage: int, amount: float, 
                 entry_price: float, update_time: int, mark_price: float = None, 
                 pnl: float = None, roe: float=None, yellow: bool = None, trade_before:bool = None) -> None:
        super().__init__()
        self.symbol = symbol
        self.position_side = TradingConstants.POSITION_SIDE_LONG \
            if amount > 0 else TradingConstants.POSITION_SIDE_SHORT
        self.leverage = leverage
        self.amount = amount
        self.entry_price = entry_price
        self.update_time = update_time
        self.mark_price = mark_price
        self.pnl = pnl
        self.roe = roe
        self.yellow = yellow
        self.trade_before = trade_before

        pass

    def is_same_type(self, pos: Self):
        return self.symbol == pos.symbol and self.position_side == pos.position_side

    def is_same_position(self, pos: Self):
        return self.symbol == pos.symbol and \
            self.leverage == pos.leverage and \
            self.position_side == pos.position_side and \
            self.amount == pos.amount and \
            self.entry_price == pos.entry_price
    