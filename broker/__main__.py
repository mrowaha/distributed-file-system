import time
import zmq
import json
import fs_events as fs
import argparse
from typing import TypedDict
from broker_logger import logger


def main(*, reset: bool = False):
    fsStore : fs.FsEventStore = fs.FsEventStore()
    fsStore.createProjectionState()
    if (reset):
        fsStore.reset()
    logger.info(f"stream name: {fsStore.fsStream}")

    context = zmq.Context()
    masterNodeSocket = context.socket(zmq.REP)
    masterNodeSocket.bind("tcp://*:5555")

    while True:
        #  Wait for next request from client
        message = masterNodeSocket.recv()
        print("Received request: %s" % message)
        masterNodeSocket.send_string(json.dumps({"status": "ok"}))


class Args(TypedDict, total=False):
    reset: bool

default_args = {
    "reset": False
}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-r", "--reset", action="store_true" ,help="reset fs event stream")
    args =  Args({**default_args, **vars(ap.parse_args())})
    main(
        reset=args['reset']
    )