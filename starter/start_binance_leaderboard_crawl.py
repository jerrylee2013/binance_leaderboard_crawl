import logging

from qtr.crawl.binance.futures_umargin.leaderboard_crawl_controller import LeaderboardCrawlController
from qtr.utils.constants import TradingConstants

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)

binanceLeaderboardCrawlController = LeaderboardCrawlController(db_url=TradingConstants.DEFAULT_DB_URL)
LOGGER.info("ready to setup")
if binanceLeaderboardCrawlController.setup():
    LOGGER.info("ready to start crawl")
    binanceLeaderboardCrawlController.start_crawl()
    LOGGER.info("crawl launched")