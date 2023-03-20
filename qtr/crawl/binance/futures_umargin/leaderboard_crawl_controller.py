import logging
import threading
import pymongo
import queue
import time
from datetime import datetime

from qtr.base.controller.monitor.service_controller import ServiceController
from qtr.crawl.binance.futures_umargin.trader_position import TraderPosition
from qtr.crawl.binance.futures_umargin.trader_position_crawl_service import TraderPositionCrawlService
from qtr.crawl.binance.futures_umargin.trader_ranks_crawl_service import TraderRanksCrawlService
from qtr.utils.constants import TradingConstants
from qtr.utils.crawl_constants import CrawlConstants

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class LeaderboardCrawlController(ServiceController):
    """
    币安的leaderboard永续合约爬虫总控器
    """
    

    def __init__(self, db_url: str,
                 rank_interval: int = 1,
                 user_info_interval: int = 2,
                 position_interval: int = 2) -> None:
        super().__init__(TradingConstants.DEFAULT_MQ_URL,
                         CrawlConstants.CRAWL_RPC_QUEUE_NAME)
        self.db_url = db_url
        self.rank_interval = rank_interval
        self.user_info_interval = user_info_interval
        self.position_interval = position_interval
        self.all_traders: list[str] = []
        self.trader_position_mapping = {}
        self.position_command_queue = queue.SimpleQueue()
        self.rank_crawl_service = TraderRanksCrawlService(self) # 排行榜爬虫
        self.trader_position_crawl_service: TraderPositionCrawlService = None # 带单人仓位爬虫

    def setup(self) -> bool:
        LOGGER.info("ready to setup crawl controller")
        if super().setup():
            try:
                self.mongoclient = pymongo.MongoClient(
                    self.db_url
                )
                db = self.mongoclient[CrawlConstants.CRAWL_FU_DB_NAME]
                self.trader_col = db["trader"]
                self.position_col = db["position"]
                self.position_op_col = db["operations"]
                traders: list[dict] = self.trader_col.find(
                    {"crawl_status": True})
                if traders is not None:
                    for trader in traders:
                        uid = trader.get("uid")
                        self.trader_position_mapping[uid] = None
                        positions: dict = self.position_col.find_one(
                            {"uid": uid})
                        if positions is not None:
                            ps = positions.get("positions")
                            self.trader_position_mapping[uid] = TraderPosition.make_position_list(
                                ps)
                if not self.rank_crawl_service.setup():
                    print("start fail due to rank crawl service setup failed")
                    return False
                self.trader_position_crawl_service = TraderPositionCrawlService(
                    self)
                return True
            except Exception as ex:
                print(ex)
                return False

    def update_crawl_intervals(self, params: dict):
        if params is not None:
            self.rank_interval = params.get("rank", self.rank_interval)
            self.user_info_interval = params.get("user", self.user_info_interval)
            self.position_interval = params.get("position", self.position_interval)
            return self.make_success_result({
                "data": True
            })
        return self.make_error_result("no config found")

    def get_crawl_status(self, params: dict):
        try:
            result = {
                "intervals": {
                    "rank": self.rank_interval,
                    "user": self.user_info_interval,
                    "position": self.position_interval
                },
                "rank_crawl": {
                    "total_trader_count": len(self.rank_crawl_service.all_crawl_trader_ids),
                    "last_rank_udpate": datetime.strftime(self.rank_crawl_service.last_rank_update, TradingConstants.TIME_FORMAT),
                    "last_rank_time_span": self.rank_crawl_service.last_rank_time.total_seconds(),
                    "last_rank_count": self.rank_crawl_service.last_rank_count,
                    "last_users_update": datetime.strftime(self.rank_crawl_service.last_performance_update, TradingConstants.TIME_FORMAT),
                    "last_users_time_span": self.rank_crawl_service.last_performance_time.total_seconds(),
                    "last_users_count": self.rank_crawl_service.last_performance_count
                },
                "position_crawl": {
                    "current_share_trader_count": len(self.all_traders),
                    "last_crawl_count": self.trader_position_crawl_service.last_crawl_count,
                    "last_crawl_fail": self.trader_position_crawl_service.last_fail_count,
                    "last_crawl_time": self.trader_position_crawl_service.last_crawl_time.total_seconds(),
                    "last_update": datetime.strftime(self.trader_position_crawl_service.last_update, TradingConstants.TIME_FORMAT),
                    "total_failed": self.trader_position_crawl_service.total_failed_times,
                    "total_times": self.trader_position_crawl_service.total_crawl_time
                }
            }
            return self.make_success_result(result)
        except Exception as ex:
            LOGGER.error(str(ex))
            return self.make_error_result("server interal error:" + str(ex))
        pass

    def process_control_task(self, method: str, params: dict):
        super().process_control_task(method, params)
        if method == "status":
            return self.get_crawl_status(params)
        elif method == "update_interval":
            return self.update_crawl_intervals(params)
        else:
            return self.make_error_result("unknown method " + method)


    def on_new_traders(self, new_trader_list: list[str]):
        self.all_traders.extend(new_trader_list)

    def on_trader_close_share(self, uid:str):
        try:
            self.all_traders.remove(uid)
        except ValueError:
            pass

    def on_trader_init_position(self, trader_id: str, positions: list[TraderPosition]):
        command = {
            "name": "new",
            "uid": trader_id,
            "positions": positions
        }
        self.position_command_queue.put_nowait(command)

    def on_trader_position_changed(self, trader_id: str, old_positions: list[TraderPosition], new_positions: list[TraderPosition], diff_ref: dict):
        command = {
            "name": "diff",
            "uid": trader_id,
            "new": new_positions,
            "old": old_positions,
            "diff": diff_ref
        }
        self.position_command_queue.put_nowait(command)

    def position_command_handle_task(self):
        while True:
            command: dict = self.position_command_queue.get()
            name = command.get("name")
            uid = command.get("uid")
            if name == "new":
                positions: list[TraderPosition] = command.get("positions", [])
                entry = {
                    "record_time":  datetime.strftime(datetime.now(),
                                                      TradingConstants.TIME_FORMAT),
                    "record_time_stamp": time.time(),
                    "uid": uid,
                    "positions": [p.__dict__.copy() for p in positions]
                }
                try:
                    self.position_col.replace_one({"uid": uid}, entry, True)
                except Exception as ex:
                    print("failed to save new position", ex)
            elif name == "diff":
                new_positions: list[TraderPosition] = command.get("new", [])
                old_positions: list[TraderPosition] = command.get("old", [])
                diff_pos: list[dict] = command.get("diff", [])
                entry = {
                    "record_time":  datetime.strftime(datetime.now(),
                                                      TradingConstants.TIME_FORMAT),
                    "record_time_stamp": time.time(),
                    "uid": uid,
                    "positions": [p.__dict__.copy() for p in new_positions]
                }
                try:
                    self.position_col.replace_one({"uid": uid}, entry, True)
                except Exception as ex:
                    print("failed to save new position", ex)
                removed: list[TraderPosition] = diff_pos.get("removed", [])
                added: list[TraderPosition] = diff_pos.get("added", [])
                changed: list[(TraderPosition, TraderPosition)
                              ] = diff_pos.get("changed", [])
                diff = {
                    "removed": [p.__dict__.copy() for p in removed],
                    "added": [[p.__dict__.copy() for p in added]],
                    "changed": [{"from": p[0].__dict__.copy(),
                                 "to":p[1].__dict__.copy()}
                                for p in changed]
                }
                entry = {
                    "record_time":  datetime.strftime(datetime.now(),
                                                      TradingConstants.TIME_FORMAT),
                    "record_time_stamp": time.time(),
                    "uid": uid,
                    "new": [p.__dict__.copy() for p in new_positions],
                    "old": [p.__dict__.copy() for p in old_positions],
                    "diff": diff
                }
                try:
                    self.position_op_col.insert_one(entry)
                except Exception as ex:
                    print("failed to save position operation failed", ex)

    def position_command_task_thread(self):
        self.position_command_handle_task()

    def start_crawl(self):
        LOGGER.info("enter start_crawl")
        self.rpc_consumer.run()
        position_handle_thread = threading.Thread(
            target=self.position_command_task_thread)
        position_handle_thread.start()
        self.rank_crawl_service.start_task()
        self.trader_position_crawl_service.start_task()
        LOGGER.info("start_crawl ready")
