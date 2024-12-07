package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mrowaha/hdfs-api/api"
	"github.com/mrowaha/hdfs-api/constants"
	"google.golang.org/grpc"
)

type HdfsDataNode struct {
	api.HdfsChunkServiceServer
	store         DataNodeStore
	me            uuid.UUID
	serviceClient api.HdfsDataNodeServiceClient
}

func NewHdfsDataNode(conn *grpc.ClientConn, store DataNodeStore) IHdfsDataNode {
	serviceClient := api.NewHdfsDataNodeServiceClient(conn)
	savedUuid, _ := store.Me()
	return &HdfsDataNode{
		serviceClient: serviceClient,
		store:         store,
		me:            savedUuid,
	}
}

func (c *HdfsDataNode) validateStore(fsState []string) {
	currFiles, err := c.store.ListFiles()
	if err != nil {
		log.Fatalf("failed to validate store %v", err)
	}
	fmt.Print(len(currFiles))
	tobeDeletedFiles := difference(currFiles, fsState)
	for _, file := range tobeDeletedFiles {
		log.Printf("deleting file %s", file)
		c.store.DeleteChunks(file)
	}
}

func (c *HdfsDataNode) HeartBeat() {
	go func() {
		ticker := time.NewTicker(constants.HEART_BEAT)
		defer ticker.Stop()
		for {
			<-ticker.C // Wait for the next tick
			meta, err := c.store.Meta()
			if err != nil {
				log.Fatalf("failed to get meta on node %s", c.me)
			}

			requestMeta := make([]*api.DataNodeFileMeta, len(meta))
			for _, fileMeta := range meta {
				requestMeta = append(requestMeta, &api.DataNodeFileMeta{
					FileName:    fileMeta.FileName,
					Chunks:      fileMeta.Chunks,
					TotalChunks: fileMeta.TotalChunks,
				})
			}
			req := &api.HeartBeatRequest{
				Id:   c.me.String(),
				Meta: requestMeta,
				Size: float32(c.store.Size()),
			}

			res, err := c.serviceClient.Heartbeat(context.Background(), req)
			if err != nil {
				log.Fatalf("failed to send heartbeat to master: %v", err)
			}
			log.Printf("heartbeat: %v", res)
		}
	}()
}

func (c *HdfsDataNode) Connect() error {
	meta, err := c.store.Meta()
	if err != nil {
		log.Fatalf("failed to get meta on node %s", c.me)
	}

	requestMeta := make([]*api.DataNodeFileMeta, len(meta))
	for _, fileMeta := range meta {
		requestMeta = append(requestMeta, &api.DataNodeFileMeta{
			FileName:    fileMeta.FileName,
			Chunks:      fileMeta.Chunks,
			TotalChunks: fileMeta.TotalChunks,
		})
	}

	var req *api.RegisterRequest
	if c.me == uuid.Nil {
		req = &api.RegisterRequest{
			New:     true,
			Size:    float32(c.store.Size()),
			Meta:    requestMeta,
			Service: os.Getenv("Service_Port"),
		}
	} else {
		req = &api.RegisterRequest{
			New:     false,
			Id:      c.me.String(),
			Size:    float32(c.store.Size()),
			Meta:    requestMeta,
			Service: os.Getenv("Service_Port"),
		}
	}

	stream, err := c.serviceClient.Connect(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to start Connect RPC: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		log.Fatalf("failed to receive register ack")
	}

	if resp.Action != api.FileAction_REGISTER {
		log.Fatalf("failed to receive register ack action")
	}

	c.validateStore(resp.FsState)
	if c.me == uuid.Nil {
		c.me, err = uuid.Parse(resp.DatanodeId)
		if err != nil {
			log.Fatalf("failed to parse data node id from master")
		}
		err := c.store.UpdateMe(c.me)
		if err != nil {
			log.Fatalf("failed to update me: %v", err)
		}
	}

	log.Printf("connected to server stream for Data Node ID: %s", req.Id)
	log.Printf("me: %s", c.me.String())

	// register first heart beat now
	c.HeartBeat()
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				log.Fatalf("stream context closed: %v", err)
			}
			if err.Error() == "EOF" {
				log.Fatalf("stream closed by server")
			}
			log.Fatalf("error receiving from stream: %v", err)
		}

		switch resp.Action {
		case api.FileAction_UPLOAD_CHUNK:
			log.Printf("action: Upload chunks, file: %s, chunk: %d", resp.FileName, resp.Chunk.ChunkNumber)
			err := c.store.Write(resp.FileName, resp.Chunk.ChunkNumber, resp.Chunk.TotalChunks, resp.Chunk.Data)
			if err != nil {
				// failed to write
				log.Fatalf("failed to write %v", err)
			}
		case api.FileAction_DELETE_FILE:
			log.Printf("action: Delete file, file %s", resp.FileName)
			err := c.store.DeleteChunks(resp.FileName)
			if err != nil {
				log.Fatalf("failed to delete")
			}
		}

	}
}

func (c *HdfsDataNode) BeginService(wg *sync.WaitGroup) {
	listener, err := net.Listen("tcp", os.Getenv("Service_Port"))
	if err != nil {
		log.Fatalf("%v", err)
	}
	server := grpc.NewServer()
	api.RegisterHdfsChunkServiceServer(server, c)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(listener); err != nil {
			log.Fatalf("chunk service failed to start: %v", err)
		}
	}()
}

func (c *HdfsDataNode) SendChunk(ctx context.Context, in *api.SendChunkRequest) (*api.SendChunkRespose, error) {
	data, err := c.store.ReadChunks(in.FileName, in.ChunkNumbers)
	if err != nil {
		return nil, err
	}

	return &api.SendChunkRespose{
		ChunkData: data,
	}, nil
}

// helpers //////////////////
func difference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}
