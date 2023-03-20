
class CrawlConstants(object):
    CRAWL_FU_DB_NAME = "binance_crawl_futures_umargin"
    CRAWL_FU_SUMMARY_DB_NAME = "binance_crawl_futures_umargin_summary"
    CRAWL_RPC_QUEUE_NAME = "quantrend.binance_futures_umargin_leaderboard_crawl_controller"


    ENABLE_CRAWL_USER_LIMIT = False
    CRAWL_USER_LIMIT = 10

    RANK_URL = "https://www.binance.com/bapi/futures/v3/public/future/leaderboard/getLeaderboardRank"
    PERFORMANCE_URL = "https://www.binance.com/bapi/futures/v2/public/future/leaderboard/getOtherPerformance"
    BASEINFO_URL = "https://www.binance.com/bapi/futures/v2/public/future/leaderboard/getOtherLeaderboardBaseInfo"
    POSITION_URL = "https://www.binance.com/bapi/futures/v1/public/future/leaderboard/getOtherPosition"

    
    pass