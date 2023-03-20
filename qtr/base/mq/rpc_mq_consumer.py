import pika
import threading
import time
import logging
import json
from typing import Callable

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from qtr.base.nonjsonable import NoneJsonable
from qtr.utils.constants import TradingConstants
from qtr.utils.json_encoder import TradingObjectEncode

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)




class RPCMQConsumer(NoneJsonable):
    """
    基于RabbitMQ的BlockingChannel封装的RPC模型
    """

    def __init__(self, mq_url: str, queue_name: str, request_handler: Callable) -> None:
        self.mq_url = mq_url
        self.queue_name = queue_name
        self.request_handler = request_handler
        self.callback_queue = None
        self.mq_connection: pika.BlockingConnection = None
        self.channel: BlockingChannel = None
        self.last_message: dict = None
        self.last_update: time = None
        pass

    def setup(self) -> bool:
        try:
            self.mq_connection = pika.BlockingConnection(
                pika.URLParameters(self.mq_url)
            )
            self.channel = self.mq_connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
            self.channel.basic_qos(prefetch_count=1)
            return True
        except Exception as ex:
            LOGGER.error(str(ex))
            return False

    """
    通用返回值格式
    {
        "ok": <boolean>,
        "data": <any>,
        "error": <string | json>
    }
    """
    def on_request(self, ch: BlockingChannel, method: Basic.Deliver, properties: pika.BasicProperties, body: bytes):
        self.last_message = {
            "time": time.strftime(
                TradingConstants.TIME_FORMAT, time.localtime(time.time())),
            "request": body

        }
        content_type = properties.content_type
        LOGGER.info("Receive:%s, %s", content_type, body)
        body_object = None
        if content_type == "application/json":
            body_object = json.loads(body)
        else:
            body_object = {
                "data": body
            }
        result_str = None
        result = {
            "ok": True,
            "data": None,
            "error": None
        }
        try:
            raw_result = self.request_handler(body_object)
            # result_str = json.dumps(raw_result, ensure_ascii=False)
            result["data"] = raw_result
        except Exception as ex:
            LOGGER.error(msg="consumer exception", exc_info=ex)
            print(ex)
            result["ok"] = False
            result["error"] = str(ex)

        result_str = json.dumps(result, ensure_ascii=False, cls=TradingObjectEncode)

        ch.basic_publish(exchange='', routing_key=properties.reply_to, properties=pika.BasicProperties(
            correlation_id=properties.correlation_id), body=result_str)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def consumer_task_thread(self):
        while True:
            try:
                if self.setup():
                    self.channel.basic_consume(
                        queue=self.queue_name, on_message_callback=self.on_request)
                    self.channel.start_consuming()
            except Exception as ex:
                print("channel exception", ex)
                LOGGER.error(msg="consumer exception", exc_info=ex)
                pass

    def run(self):
        consumer_task_thread = threading.Thread(target=self.consumer_task_thread)
        consumer_task_thread.start()
