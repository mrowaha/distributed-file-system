package main

import (
	"io"
	"log"

	pb "github.com/mrowaha/hdfs-api/api"
	"google.golang.org/grpc"
)

type master struct {
	pb.UnimplementedHdfsMasterServiceServer
}

func (s *master) CreateFile(stream grpc.ClientStreamingServer[pb.CreateFileRequest, pb.CreateFileResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Client has closed the stream without an error
				log.Printf("Client finished sending chunks")
				break
			}
			log.Printf("Stream ended: %v", err)
			break
		}

		// Process the file chunk
		log.Printf("Received chunk %d for file %s", req.Chunk.ChunkNumber, req.FileName)
	}
	return nil
}
