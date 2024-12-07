import fs_events as fs
import master_socket as ms
import argparse
from typing import TypedDict
from broker_logger import logger


def main(*, reset: bool = False, list: bool = False):
    fsStore : fs.FsEventStore = fs.FsEventStore()
    fsStore.createProjectionState()
    if (reset):
        fsStore.reset()

    if (list):
        print(fsStore.events())
    logger.info(f"stream name: {fsStore.fsStream}")

    def createFileHandler(data: any):
        fsStore.createFile(data["file_name"], data["total_chunks"])
        return {"status": "created"}

    def getCurrentFsState(data: any):
        state = fsStore.currentState()
        logger.info(f"projection state: {state}")
        if state is None:
            return {"state": []}
        return state

    def deleteFileHandler(data: any):
        fsStore.deleteFile(data["file_name"])
        return {"status": "deleted"}        

    masterSock = ms.MasterSocker()
    masterSock.registerHandler(
        ms.MasterRequest.CREATE_FILE.value,
        createFileHandler
    )
    masterSock.registerHandler(
        ms.MasterRequest.CURR_STATE.value,
        getCurrentFsState
    )
    masterSock.registerHandler(
        ms.MasterRequest.DELETE_FILE.value,
        deleteFileHandler
    )
    masterSock.listen()


class Args(TypedDict, total=False):
    reset: bool
    list: bool

default_args = {
    "reset": False,
    "list": False
}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-r", "--reset", action="store_true" ,help="reset fs event stream")
    ap.add_argument("-ls","--list", action="store_true", help="print events")
    args =  Args({**default_args, **vars(ap.parse_args())})
    main(
        reset=args['reset'],
        list=args['list']
    )