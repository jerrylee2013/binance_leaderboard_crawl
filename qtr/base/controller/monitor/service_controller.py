import logging

from qtr.base.mq.rpc_mq_consumer import RPCMQConsumer

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

class ServiceController(object):
    """
    对外暴露Service对象的管理控制类，每个ServiceController管理一个Service对象。
    具体的Service类将负责定制自己的ServiceController
    """

    def __init__(self, mq_url:str, queue_name: str) -> None:
        self.mq_url = mq_url
        self.queue_name = queue_name
        self.rpc_consumer = RPCMQConsumer(self.mq_url, self.queue_name, self.on_request)
        pass

    def setup(self) -> bool:
        if not self.rpc_consumer.setup():
            LOGGER.info("rpc consumer setup failed " + self.mq_url + "," + self.queue_name)
            return False
        return True

    def run(self):
        self.rpc_consumer.run()

    def on_request(self, request: dict):
        request_data: dict = request.get("data")
        method: str = request_data.get("method")
        params: dict = request_data.get("params")
        return self.process_control_task(method, params)

    def make_success_result(self, result: dict):
        return {
            "success": True,
            "data": result
        }

    def make_error_result(self, reason: str):
        return {
            "success": False,
            "message": reason
        }

    def process_control_task(self, method: str, params:dict):
        LOGGER.info("processing " + method + " with params:" + str(params))
        pass
    