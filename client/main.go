package main

import (
	"log"
	"os"

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
	action := os.Getenv("TYPE")
	if action == "1" {
		_ = client.CreateFile("image.jpeg", "image.jpeg")
	} else if action == "2" {
		err := client.DeleteFile("image.jpeg")
		if err != nil {
			log.Printf("%v", err)
		}
	} else if action == "3" {
		resp, _ := client.FetchNodes("image.jpeg")
		client.GetChunksFromNodes("image.jpeg", int(resp.TotalChunks), resp.Nodes)
	}
}
