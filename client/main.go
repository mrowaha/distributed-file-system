package main

import (
	"log"

	hdfsClient "github.com/mrowaha/hdfs-api/client"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := hdfsClient.NewHdfsClientImpl(conn)
	_ = client.CreateFile("sky.jpeg", "sky.jpeg")
}
