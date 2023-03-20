
from datetime import datetime, timedelta

class TradingConstants(object):
    """
    此处定义了交易系统中所有的常量
    """

    # 交易所名称
    EXCHANGE_BINANCE = "binance"
    BNB_FEE_UNIT_NAME = "BNB"

    DEPLOY_ENV_TEST = "test"
    DEPLOY_ENV_PRODUCT = "product"
    DEPLOY_ENV_STAGING = "staging"

    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    # mq的缺省url
    DEFAULT_MQ_URL = "amqp://guest:guest@127.0.0.1:5672/%2F?heartbeat=7200&" + \
        "blocked_connection_timeout=300"
    # 缺省mongodb的url
    DEFAULT_DB_URL = "mongodb://localhost:27017/"


    # 交易类型
    TRADE_TYPE_FUTURES_U_MARGINS = "FU"  # U本位合约
    TRADE_TYPE_SPOT = "S"   # 现货

    # 订单相关字段
    POSITION_SIDE_LONG = "LONG"
    POSITION_SIDE_SHORT = "SHORT"
    SIDE_BUY = "BUY"
    SIDE_SELL = "SELL"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    ORDER_TYPE_TAKE_PROFIT_LIMIT ="TAKE_PROFIT_LIMIT"

    
    def get_current_time_in_format() -> str:
        return datetime.strftime(datetime.now(), TradingConstants.TIME_FORMAT)
    
    def convert_timestamp_to_str(stamp):
        # 把币安交易所返回的时间戳转成字符串
        return datetime.strftime(datetime.fromtimestamp(stamp / 1000), TradingConstants.TIME_FORMAT)

    def convert_timedelta_to_string(delta: timedelta):
        time_span = str(delta.days) + "天"
        hours, remainder = divmod(delta.seconds, 3600)
        time_span += str(hours) + "小时"
        minutes, seconds = divmod(remainder, 60)
        time_span += str(minutes) + "分钟" + str(seconds) + "秒"
        return time_span
    pass

