import json
import zmq
from enum import Enum
from broker_logger import logger

class MasterRequest(Enum):
    CREATE_FILE = "create_file"
    CURR_STATE = "curr_state"
    DELETE_FILE = "delete_file"

class MasterSocker:
    def __init__(self):
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.REP)
        self.sock.bind("tcp://*:5555")
        self.handlers = {}

    def registerHandler(self, event: MasterRequest, handler) -> None:
        self.handlers[event] = handler


    def listen(self):
        logger.info("master sock listening on :5555")
        while True:
            data = self.sock.recv()
            req = json.loads(data.decode('utf-8'))
            event = req["event"]
            res = self.route(event, req)
            self.sock.send_json(res)
    
    def route(self, event: MasterRequest, data: any) -> any:
        handler = self.handlers[event]
        return handler(data)
