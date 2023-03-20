from __future__ import annotations
import typing
if typing.TYPE_CHECKING:
    from qtr.crawl.binance.futures_umargin.leaderboard_crawl_controller import LeaderboardCrawlController

import requests
import schedule
import threading
import pymongo
import time
import logging
from datetime import datetime, timedelta
from qtr.base.jsonable import Jsonable
from qtr.utils.constants import TradingConstants
from qtr.utils.crawl_constants import CrawlConstants

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class TraderRanksCrawlService(Jsonable):
    
    def __init__(self, controller: LeaderboardCrawlController) -> None:
        super().__init__()
        self.controller = controller
        self.last_rank_time: timedelta = timedelta(seconds=0)
        self.last_performance_time: timedelta = timedelta(seconds=0)
        self.last_rank_update: datetime = datetime.now() - timedelta(seconds=100000)
        self.last_rank_count = 0
        self.last_performance_update: datetime = datetime.now() - timedelta(seconds=100000)
        self.last_performance_count = 0
        self.all_crawl_trader_ids = [] # 曾经在排行榜出现的所有uid，本爬虫将爬取所有用户信息以及战绩数据
        self.trader_share_mapping = {

        }
        self.task_lock = threading.BoundedSemaphore(1)

    def build_all_trader_info(self):
        # 此处是从总结库中获取最新结果
        binance_crawl_summary_db = self.mongoclient[CrawlConstants.CRAWL_FU_SUMMARY_DB_NAME]
        binance_trader_col = binance_crawl_summary_db["trader"]
        binance_trader_board_info_col = binance_crawl_summary_db["board_info"]
        all_trader: list[dict] = binance_trader_col.find()
        all_shared_traders:list[str] = []
        for trader in all_trader:
            uid = trader.get("uid")
            self.all_crawl_trader_ids.append(uid)
            board_info: dict = binance_trader_board_info_col.find_one({
                                                                      "uid": uid})
            position_shared = False
            if board_info is not None:
                position_shared = board_info.get("base_info", {})\
                    .get("positionShared", False)
                if position_shared:
                    all_shared_traders.append(uid)
            self.trader_share_mapping[uid] = position_shared
        self.controller.on_new_traders(all_shared_traders)
        LOGGER.info("current all traders count " + str(len(self.all_crawl_trader_ids)))

    def setup(self):
        try:
            self.mongoclient = pymongo.MongoClient(self.controller.db_url)
            db = self.mongoclient[CrawlConstants.CRAWL_FU_DB_NAME]
            summary_db = self.mongoclient[CrawlConstants.CRAWL_FU_SUMMARY_DB_NAME]
            self.trader_col = db["trader"]
            self.trader_summary_col = summary_db["trader"]
            self.trader_rank_col = db["rank"]
            self.trader_rank_summary_col = summary_db["rank"]
            self.trader_board_info_col = db["board_info"]
            self.trader_board_info_summary_col = summary_db["board_info"]
            self.trader_performance_col = db["performance"]
            self.trader_performance_summary_col = summary_db["performance"]
            self.build_all_trader_info()

            return True
        except Exception as ex:
            print(ex)
            return False
        pass

    def has_trader(self, uid: str):
        try:
            self.all_crawl_trader_ids.index(uid)
            return True
        except ValueError as er:
            return False
        
    def has_share_trader(self, uid:str):
        return self.trader_share_mapping.get(uid, False)
        
    def add_new_trader(self, trader: dict):
        now = datetime.strftime(datetime.now(), TradingConstants.TIME_FORMAT)
        entry = {
            "uid": trader.get("uid"),
            "nickname": trader.get("nickname"),
            "crawl_status": True,
            "create_at": now,
            "last_update": now
        }
        try:
            self.trader_col.insert_one(entry)
            self.trader_summary_col.insert_one(entry)
            return True
        except Exception as ex:
            LOGGER.error("save new trader failed " + str(ex))
            print("save new trader failed", ex)
            return False
        pass

    def clear_rank_summary(self):
        try:
            self.trader_rank_summary_col.delete_many({"record_time_stamp": {"$lt": time.time()}})
        except Exception as ex:
            LOGGER.error("save trader rank failed " + str(ex))
            print("save trader rank error", ex)
        pass

    def save_rank_result(self, payload: dict, rank_list: list[dict]):
        try:
            entry = {
                "record_time":  datetime.strftime(datetime.now(), 
                                                               TradingConstants.TIME_FORMAT),
                "record_time_stamp": time.time(),
                "payload": payload,
                "rank_list": rank_list
            }
            self.trader_rank_col.insert_one(entry)
            self.trader_rank_summary_col.insert_one(entry)
        except Exception as ex:
            LOGGER.error("save trader rank failed " + str(ex))
            print("save trader rank error", ex)
        pass

    def do_rank_list_fetch(self, payload):
        LOGGER.info("do_rank_list_fetch " + str(payload))
        new_share_traders: list[str] = []
        try:
            list_response = requests.post(
                CrawlConstants.RANK_URL, json=payload)
            list_result: dict = list_response.json()
            if list_result.get("success", False):
                self.last_rank_count += 1
                data = list_result.get("data")
                if data is not None:
                    kv_list: list[dict] = data
                    self.save_rank_result(payload, kv_list)
                    for kv in kv_list:
                        e_uid = kv.get("encryptedUid")
                        nickname = kv.get("nickName")
                        position_shared = kv.get("positionShared", False)
                        trader = {
                            "uid": e_uid,
                            "nickname": nickname
                        }
                        if not self.has_trader(e_uid) and self.add_new_trader(trader):
                            self.all_crawl_trader_ids.append(e_uid)
                        
                        if position_shared and not self.has_share_trader(e_uid):
                            new_share_traders.append(e_uid)

                    if len(new_share_traders) > 0:
                        self.controller.on_new_traders(new_share_traders)

        except requests.RequestException as re:
            print("rank list fetch failed", payload, re)
            LOGGER.error("rank list fetch failed" + str(payload) + ";" + str(re))
    
    def fetch_trader_ranks(self):
        LOGGER.info("start fetch_trader_ranks")
        self.task_lock.acquire()
        if self.last_rank_update is not None:
            last_span = datetime.now() - self.last_rank_update
            if last_span.total_seconds() < 60 * 10:
                self.task_lock.release()
                LOGGER.info("exit fetch_trader_ranks too fast")
                return
        self.clear_rank_summary()
        start_time = datetime.now()
        self.last_rank_count = 0
        share_options = [True, False]
        trader_options = [True, False]
        periodTypes = ["DAILY", "WEEKLY", "MONTHLY", "ALL"]
        statisticsTypes = ["PNL", "ROI"]
        for share_option in share_options:
            for trader_option in trader_options:
                if share_option and trader_option: # 这两个参数为True时，返回的的数据是[]，不知何故。
                    continue
                for pt in periodTypes:
                    for st in statisticsTypes:
                        # 按日收益额的排名
                        payload = {
                            "isShared": share_option,
                            "isTrader": trader_option,
                            "periodType": pt,
                            "statisticsType": st,
                            "tradeType": "PERPETUAL"  # "PERPETUAL", "DELIVERY"
                        }
                        self.do_rank_list_fetch(payload)
                        time.sleep(self.controller.rank_interval)
        end_time = datetime.now()
        self.last_rank_time = end_time - start_time
        self.last_rank_update = datetime.now()
        self.task_lock.release()
        LOGGER.info("exit fetch_trader_ranks normally")

    def save_trader_performance(self, uid: str, performance: dict):
        try:
            entry = {
                "record_time":  datetime.strftime(datetime.now(), TradingConstants.TIME_FORMAT),
                "record_time_stamp": time.time(),
                "uid": uid,
                "performance": performance
            }
            self.trader_performance_col.insert_one(entry)
            del entry["_id"]
            self.trader_performance_summary_col.replace_one({"uid": uid}, entry, upsert=True)
        except Exception as ex:
            print("save trader performance error", ex)
        pass

    def save_trader_baseinfo(self, uid: str, baseinfo: dict):
        try:
            entry = {
                "record_time":  datetime.strftime(datetime.now(), TradingConstants.TIME_FORMAT),
                "record_time_stamp": time.time(),
                "uid": uid,
                "base_info": baseinfo
            }
            self.trader_board_info_col.insert_one(entry)
            del entry["_id"]
            self.trader_board_info_summary_col.replace_one({"uid": uid}, entry, upsert=True)
            positionShared = baseinfo.get("positionShared")
            old_shared = self.trader_share_mapping.get(uid)
            if old_shared is not None:
                if old_shared and not positionShared:
                    # 用户关闭仓位共享
                    self.controller.on_trader_close_share(uid)
            self.trader_share_mapping[uid] = positionShared

        except Exception as ex:
            print("save trader base info error", ex)
        pass

    def do_trader_info_fetch(self, uid: str):
        payload = {
            "encryptedUid": uid,
            "tradeType": "PERPETUAL"
        }
        try:
            response = requests.post(
                CrawlConstants.PERFORMANCE_URL, json=payload)
            result: dict = response.json()
            if result.get("success", False):
                data = result.get("data")
                self.save_trader_performance(uid, data)

        except requests.RequestException as re:
            print("trader performance fetch failed", payload, re)
        payload = {
            "encryptedUid": uid
        }
        try:
            response = requests.post(
                CrawlConstants.BASEINFO_URL, json=payload)
            result: dict = response.json()
            if result.get("success", False):
                data = result.get("data")
                self.save_trader_baseinfo(uid, data)
        except requests.RequestException as re:
            print("trader base info fetch failed", payload, re)
        pass

    def fetch_trader_info(self):
        LOGGER.info("start fetch_trader_info")
        self.task_lock.acquire()
        if self.last_performance_update is not None:
            last_span = datetime.now() - self.last_performance_update
            if last_span.total_seconds() < 60 * 10:
                self.task_lock.release()
                LOGGER.info("exit fetch_trader_info too fast")
                return
        start_time = datetime.now()
        self.last_performance_count = 0
        traders: list[str] = self.all_crawl_trader_ids.copy()
        if CrawlConstants.ENABLE_CRAWL_USER_LIMIT:
            traders = traders[0: CrawlConstants.CRAWL_USER_LIMIT]
        for trader_id in traders:
            self.do_trader_info_fetch(trader_id)
            self.last_performance_count += 1
            time.sleep(self.controller.user_info_interval)
        end_time = datetime.now()
        self.last_performance_time = end_time - start_time
        self.last_performance_update = datetime.now()
        self.task_lock.release()
        LOGGER.info("exit fetch_trader_info normally")

    def crawl_task(self):
        self.fetch_trader_ranks()
        self.fetch_trader_info()
        schedule.every().day.at("02:00").do(self.fetch_trader_ranks)
        schedule.every().day.at("06:00").do(self.fetch_trader_info)
        while True:
            schedule.run_pending()
            time.sleep(10)
        pass

    def start_task(self):
        crawl_thread = threading.Thread(target=self.crawl_task)
        crawl_thread.start()
        LOGGER.info("rank crawl ready")