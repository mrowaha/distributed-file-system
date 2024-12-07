package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

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

func (c *hdfsClientImpl) DeleteFile(fileName string) error {
	request := &api.DeleteFileRequest{
		FileName: fileName,
	}

	// Call the DeleteFile RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	response, err := c.serviceClient.DeleteFile(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to delete file %s: %w", fileName, err)
	}

	// Check the response status
	if response.Status {
		log.Printf("file %s deleted successfully", fileName)
	} else {
		log.Printf("failed to delete file %s", fileName)
	}

	return nil
}

func (c *hdfsClientImpl) FetchNodes(fileName string) (*api.NodesWithChunksResponse, error) {
	request := &api.NodesWithChunksRequest{
		FileName: fileName,
	}

	// Call the DeleteFile RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	response, err := c.serviceClient.ReadNodesWithChunks(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to delete file %s: %w", fileName, err)
	}

	fmt.Printf("nodes containing chunks, %v\n", response)
	return response, nil
}

func (c *hdfsClientImpl) GetChunksFromNodes(fileName string, totalChunks int, nodes []*api.NodesWithChunksResponse_NodeWithChunk) {

	assignedChunks := make(map[string][]int64)
	services := make([]string, 0)
	for _, node := range nodes {
		assignedChunks[node.Service] = make([]int64, 0)
		services = append(services, node.Service)
	}

	currServiceIdx := 0
	for i := 1; i <= totalChunks; i++ {
		assignedChunk := assignedChunks[services[currServiceIdx]]
		assignedChunk = append(assignedChunk, int64(i))
		assignedChunks[services[currServiceIdx]] = assignedChunk
		currServiceIdx = (currServiceIdx + 1) % len(services)
	}

	wg := &sync.WaitGroup{}

	chunkData := make(map[int64][]byte)
	for k, v := range assignedChunks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.GetChunks(fileName, v, k)
			if err == nil {
				for _, chunkRes := range res {
					chunkData[chunkRes.Number] = chunkRes.Data
				}
			}
		}()
	}
	wg.Wait()

	orderedChunkData := make([][]byte, len(chunkData))
	for k, v := range chunkData {
		orderedChunkData[k-1] = v
	}

	c.MergeAndWriteFile(orderedChunkData, "image_new", "jpeg")
}

func (c *hdfsClientImpl) GetChunks(fileName string, chunks []int64, service string) ([]*api.SendChunkRespose_ChunkData, error) {
	conn, _ := grpc.Dial(fmt.Sprintf("localhost%s", service), grpc.WithInsecure())
	defer conn.Close()

	dataClient := api.NewHdfsChunkServiceClient(conn)
	resp, err := dataClient.SendChunk(context.Background(), &api.SendChunkRequest{
		FileName:     fileName,
		ChunkNumbers: chunks,
	})
	if err != nil {
		log.Fatalf("%v", err)
		return nil, err
	}
	return resp.ChunkData, nil
}

func (c *hdfsClientImpl) MergeAndWriteFile(data [][]byte, fileName, fileExtension string) error {
	fullFileName := fmt.Sprintf("%s.%s", fileName, fileExtension)

	file, err := os.Create(fullFileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	for _, chunk := range data {
		if _, err := file.Write(chunk); err != nil {
			return fmt.Errorf("failed to write data to file: %w", err)
		}
	}

	return nil
}
