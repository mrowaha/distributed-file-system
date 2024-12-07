package main

import (
	"log"
	"sync"

	"google.golang.org/grpc"

	hdfsdataNode "github.com/mrowaha/hdfs-api/node"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	datanodeStore := NewDataNodeSqlStore()
	datanodeStore.BootStrap()

	datanode := hdfsdataNode.NewHdfsDataNode(conn, datanodeStore)

	wg := &sync.WaitGroup{}
	datanode.BeginService(wg)
	datanode.Connect()
	wg.Wait()
}
