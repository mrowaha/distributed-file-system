import time
import zmq
import json
import esdbclient
from uuid  import uuid4

esdb = esdbclient.EventStoreDBClient(
    uri="esdb://admin:changeit@localhost:2113?tls=false"
)
event = esdbclient.NewEvent(
    id=uuid4(),
    type="TestEvent",
    data=b"test event"
)


esdb.append_to_stream(
    "hdfs-file-system",
    events=[event],
    current_version=esdbclient.StreamState.ANY
)

context = zmq.Context()
masterNodeSocket = context.socket(zmq.REP)
masterNodeSocket.bind("tcp://*:5555")
print("master node broker listening on port 5555")

while True:
    #  Wait for next request from client
    message = masterNodeSocket.recv()
    print("Received request: %s" % message)

    #  Do some 'work'
    time.sleep(1)
    events = esdb.get_stream("hdfs-file-system")
    for event in events:
        print(event)
    #  Send reply back to client
    masterNodeSocket.send_string(json.dumps({"status": "ok"}))
