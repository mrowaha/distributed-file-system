package main

import (
	hdfsMaster "github.com/mrowaha/hdfs-api/master"
)

func main() {

	zmqBroker := NewZmqBrokerClient()
	master := hdfsMaster.NewHdfsMasterServiceImpl(zmqBroker)
	master.Listen(":50051")
	master.Serve()
}
