from __future__ import annotations
import typing
if typing.TYPE_CHECKING:
    from qtr.crawl.binance.futures_umargin.leaderboard_crawl_controller import LeaderboardCrawlController

import requests
import threading
import time
import logging
from datetime import datetime, timedelta
from qtr.base.jsonable import Jsonable

from qtr.crawl.binance.futures_umargin.trader_position import TraderPosition
from qtr.utils.crawl_constants import CrawlConstants

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class TraderPositionCrawlService(Jsonable):

    def __init__(self, controller: LeaderboardCrawlController) -> None:
        super().__init__()
        self.controller = controller
        self.last_crawl_time: timedelta = timedelta(seconds=0)
        self.last_update: datetime = datetime.now() - timedelta(seconds=100000)
        self.total_crawl_time = 0
        self.total_success_times = 0
        self.total_failed_times = 0
        self.last_crawl_count = 0
        self.last_fail_count = 0

    def do_position_diff_check(self, uid: str, new_positions_list: list[TraderPosition]):
        LOGGER.debug("do_position_diff_check")
        current_positions: list[TraderPosition] = self.controller.trader_position_mapping.get(
            uid)
        if current_positions is None:
            LOGGER.debug("do_position_diff_check new !")
            self.controller.on_trader_init_position(uid, new_positions_list)
            self.controller.trader_position_mapping[uid] = new_positions_list[::]
            return

        current_positions = current_positions[0: len(current_positions)]
        new_positions: list[TraderPosition] = new_positions_list[0: len(
            new_positions_list)]
        position_changed = False
        diff_ref = {
            "changed": [],
            "removed": [],
            "added": []
        }
        if len(new_positions) > 0:
            same_list: list[TraderPosition] = []
            changed_list: list[TraderPosition] = []
            add_list: list[TraderPosition] = []
            removed: list[TraderPosition] = []
            for pos in new_positions_list:
                cp: TraderPosition = next(
                    (p for p in current_positions if p.is_same_type(pos)), None)
                if cp is not None:
                    if cp.is_same_position(pos):
                        # 仓位未变
                        same_list.append(cp)
                    else:
                        # 仓位变动
                        changed_list.append((cp, pos))
                        position_changed = True
                    current_positions.remove(cp)
                    new_positions.remove(pos)
                else:
                    # 新增仓位
                    add_list.append(pos)
                    position_changed = True
            if len(current_positions) > 0:
                # 未匹配的仓位表示被删除的仓位
                position_changed = True
                removed.extend(current_positions)
            if position_changed:
                LOGGER.debug("do_position_diff_check changed!")
                diff_ref["changed"] = changed_list
                diff_ref["added"] = add_list
                diff_ref["removed"] = removed
                self.controller.on_trader_position_changed(
                    uid, self.controller.trader_position_mapping.get(uid),
                    new_positions_list, diff_ref)
                self.controller.trader_position_mapping[uid] = new_positions_list[0: len(
                    new_positions_list)]
        elif len(current_positions) > 0:
            LOGGER.debug("do_position_diff_check cleared!")
            diff_ref["removed"].extend(current_positions)
            self.controller.on_trader_position_changed(
                uid, self.controller.trader_position_mapping.get(uid),
                new_positions_list, diff_ref)
            self.controller.trader_position_mapping[uid] = []
        pass

    def fetch_trader_position(self):
        self.running = True
        self.last_crawl_count = 0
        self.last_fail_count = 0
        self.total_crawl_time += 1
        # self.controller.trader_list_lock.acquire()
        my_traders = self.controller.all_traders.copy()
        # self.controller.trader_list_lock.release()
        if CrawlConstants.ENABLE_CRAWL_USER_LIMIT:
            my_traders = my_traders[0: CrawlConstants.CRAWL_USER_LIMIT]
        LOGGER.info("start new round of user position crawl, count: " + str(len(my_traders)))
        start_time = datetime.now()
        has_error = False
        for kv in my_traders:
            try:
                position_response = requests.post(CrawlConstants.POSITION_URL,
                                                  json={
                                                      "encryptedUid": kv,
                                                      "tradeType": "PERPETUAL"
                                                  })
                time.sleep(self.controller.position_interval)
                self.last_crawl_count += 1
                if position_response.ok:
                    # print(position_response.headers)
                    if position_response.status_code == 200:
                        self.total_success_times += 1
                        result: dict = position_response.json()
                        # LOGGER.info(str(result))
                        position_result: dict = result.get("data")
                        if position_result is not None:
                            position_list = position_result.get(
                                "otherPositionRetList", [])
                            if position_list is not None:
                                new_positions = TraderPosition.make_position_list(
                                    position_list)
                                self.do_position_diff_check(kv, new_positions)
                    else:
                        print("not know how to handle ", position_response.status_code, str(
                            position_response))
                else:
                    self.last_fail_count += 1
                    self.total_failed_times += 1
                    if position_response.status_code == 403:
                        has_error = True
                        print("failed", self.last_fail_count,
                              self.total_failed_times)
            except Exception as ex:
                LOGGER.error(str(ex))
                print(ex)
                continue
        end_time = datetime.now()
        self.last_crawl_time = end_time - start_time
        self.last_update = datetime.now()
        self.running = False
        LOGGER.info("user position crawl round done")

    def crawl_task(self):
        while True:
            self.fetch_trader_position()
            time.sleep(5)

    def start_task(self):
        crawl_thread = threading.Thread(target=self.crawl_task)
        crawl_thread.start()
        LOGGER.info("position crawl ready")
