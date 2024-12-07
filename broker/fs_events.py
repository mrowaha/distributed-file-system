import esdbclient
import esdbclient.exceptions as esdbexceptions
from uuid import uuid4
from enum import Enum
import json
from broker_logger import logger
from typing import Union, Sequence
import os
import time
import requests

class FsEvent(Enum):
    CREATE_FILE = "create_file"
    DELETE_FILE = "delete_file"

class FsEventStore:

    SnapshotProjection = "files"

    def __init__(self) -> "FsEventStore":
        self.esdb : esdbclient.EventStoreDBClient = esdbclient.EventStoreDBClient(
            uri="esdb://admin:changeit@localhost:2113?tls=false"
        )
        self.fsStream : str = f"brokerfs@{self.fsStreamVersion}"
        self.originalStreamPosition : Union[int, esdbclient.StreamState] = esdbclient.StreamState.NO_STREAM

    def events(self) -> Sequence[esdbclient.RecordedEvent]:
        return self.esdb.get_stream(self.fsStream)

    def createProjectionState(self) -> bool:
        try:
            self.esdb.create_projection(
                name=FsEventStore.SnapshotProjection,
                query=self.snapshotProjectionQuery
            )
            return True
        except esdbexceptions.AlreadyExists:
            return False

    def currentState(self) -> Union[any, None]:
        response = requests.get(
            f"http://localhost:2113/projection/{FsEventStore.SnapshotProjection}/state",
            auth=("admin", "changeit"),
        )

        if response.status_code == 200:
            if int(response.headers["Content-Length"]) == 0:
                return []
            return response.json()
        else:
            logger.error(f"failed to query projection: {response.text}")
            return None


    @property
    def fsStreamVersion(self) -> str:
        if os.path.exists(".stream-timestamp"):
            with open(".stream-timestamp", "r") as versionReader:
                return versionReader.read().strip()
        else:
            temp = time.time()
            with open(".stream-timestamp", "w") as versionWriter:
                versionWriter.write(str(temp))
            return str(temp)

    @property
    def snapshotProjectionQuery(self) -> str:
        initial = f'fromAll()'
        return initial + """
        .when({
            $init: function() {
                return [];
            },
            create_file: function(state, event) {
                const existingFile = state.find(file => file.file_name === event.data.file_name);
                if (!existingFile) {
                    state.push({
                        file_name: event.data.file_name,
                        total_chunks: event.data.total_chunks || 0
                    });
                }
                return state;
            },
            delete_file: function(state, event) {
                const index = state.findIndex(file => file.file_name === event.data.file_name);
                if (index > -1) {
                    state.splice(index, 1);
                }
                return state;
            }
        });
        """

    def reset(self) -> None:
        try:
            self.esdb.tombstone_stream(
                self.fsStream,
                current_version=esdbclient.StreamState.ANY
            )

            os.remove(".stream-timestamp")
            self.fsStream : str = f"brokerfs@{self.fsStreamVersion}"
            self.originalStreamPosition : Union[int, esdbclient.StreamState] = esdbclient.StreamState.NO_STREAM
            logger.info(f"new fs stream version: {self.fsStream}")

            self.esdb.update_projection(
                name=FsEventStore.SnapshotProjection,
                query=self.snapshotProjectionQuery,
            )
        except esdbexceptions.NotFound as e:
            logger.info("Hdfs stream not found")
        except esdbexceptions.StreamIsDeleted:
            logger.info("hdfs stream already deleted")

    def latestEvent(self) -> Union[esdbclient.RecordedEvent, None]:
        try:
            event = self.esdb.get_stream(self.fsStream, backwards=True, limit=1)
            return event[0]
        except esdbexceptions.NotFound:
            logger.error("latest event cannot be extract, stream does not exist")
            return None
        except esdbexceptions.StreamIsDeleted:
            logger.info("latest event cannot be extracted, steam is deleted")
            return None

    def createFile(self, fileName: str, totalChunks: int):
        logger.info(f"creating file, {fileName}")
        event = esdbclient.NewEvent(
            id=uuid4(),
            type=FsEvent.CREATE_FILE.value,
            data=bytes(json.dumps({"file_name": fileName, "total_chunks": totalChunks}), encoding='utf-8')
        )
        try:
            self.esdb.append_to_stream(
                stream_name=self.fsStream,
                events=[event],
                current_version=esdbclient.StreamState.ANY
            )
        except esdbexceptions.NotFound:
            self.esdb.append_to_stream(
                stream_name=self.fsStream,
                events=[event],
                current_version=esdbclient.StreamState.NO_STREAM
        )
            
    def deleteFile(self, fileName: str):
        logger.info(f"delete file, {fileName}")
        event = esdbclient.NewEvent(
            id=uuid4(),
            type=FsEvent.DELETE_FILE.value,
            data=bytes(json.dumps({"file_name": fileName}), encoding='utf-8')
        )
        try:
            self.esdb.append_to_stream(
                stream_name=self.fsStream,
                events=[event],
                current_version=esdbclient.StreamState.ANY
            )
        except esdbexceptions.NotFound:
            self.esdb.append_to_stream(
                stream_name=self.fsStream,
                events=[event],
                current_version=esdbclient.StreamState.NO_STREAM
        )