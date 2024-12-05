package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	api "github.com/mrowaha/hdfs-api/api"
	constants "github.com/mrowaha/hdfs-api/constants"
	"google.golang.org/grpc"
)

type hdfsClientImpl struct {
	serviceClient api.HdfsMasterServiceClient
}

func NewHdfsClientImpl(conn *grpc.ClientConn) HdfsClient {
	serviceClient := api.NewHdfsMasterServiceClient(conn)
	return &hdfsClientImpl{
		serviceClient: serviceClient,
	}
}

func (c *hdfsClientImpl) CreateFile(filePath string, fileName string) error {
	logger := log.New(os.Stdout, fmt.Sprintf("[CreateFile %s]", filePath), 0)
	if _, err := os.Stat(filePath); err != nil {
		logger.Printf("could not get file stats: %s\n", err.Error())
		return err
	}

	fileHandle, err := os.Open(filePath)
	if err != nil {
		logger.Printf("failed to open file: %v", err)
		return err
	}
	defer fileHandle.Close()

	info, err := fileHandle.Stat()
	if err != nil {
		logger.Printf("could not get file handle stats: %v", err)
		return err
	}

	// Calculate the number of chunks
	totalChunks := info.Size() / constants.CHUNK_SIZE
	if info.Size()%constants.CHUNK_SIZE != 0 {
		totalChunks++
	}

	buf := make([]byte, constants.CHUNK_SIZE)

	stream, err := c.serviceClient.CreateFile(context.Background())
	if err != nil {
		logger.Printf("failed to establish CreateFile rpc: %v", err)
		return err
	}

	chunkNumber := int64(1)
	for {
		bytesRead, err := fileHandle.Read(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			logger.Printf("failed to read from file: %v", err)
			return err
		}

		logger.Printf("chunk bytes read %d", bytesRead)
		if bytesRead == 0 {
			break
		}

		req := &api.CreateFileRequest{
			FileName:    fileName,
			TotalChunks: totalChunks,
			Chunk: &api.FileChunk{
				ChunkNumber: chunkNumber,
				Data:        buf[:bytesRead],
			},
		}
		if err := stream.Send(req); err != nil {
			logger.Printf("failed to send chunk %d: %v", chunkNumber, err)
			return err
		}
		chunkNumber++
	}

	// Close the stream and get the response
	_, err = stream.CloseAndRecv()
	if err == io.EOF || err == nil {
		return nil
	}

	logger.Printf("failed to receive close response: %v", err)
	return err
}
