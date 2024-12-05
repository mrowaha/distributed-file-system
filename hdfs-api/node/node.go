package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mrowaha/hdfs-api/api"
	"google.golang.org/grpc"
)

type HdfsDataNode struct {
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

func (c *HdfsDataNode) heartBeat(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		ctx := context.Background()
		// now start
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		defer wg.Done()
		for {
			select {
			case <-ticker.C: // Wait for the next tick
				meta, err := c.store.Meta()
				if err != nil {
					log.Fatalf("failed to get meta on node %s", c.me)
				}

				requestMeta := make([]*api.DataNodeFileMeta, len(meta))
				for _, fileMeta := range meta {
					requestMeta = append(requestMeta, &api.DataNodeFileMeta{
						FileName: fileMeta.FileName,
						Chunks:   fileMeta.Chunks,
					})
				}
				req := &api.HeartBeatRequest{
					Id:   c.me.String(),
					Meta: requestMeta,
					Size: float32(c.store.Size()),
				}

				res, err := c.serviceClient.Heartbeat(ctx, req)
				if err != nil {
					log.Printf("failed to send heartbeat to master: %v", err)
					wg.Done()
					return // Exit the function on error
				}

				log.Printf("heartbeat: %v", res)

			case <-ctx.Done(): // Handle context cancellation if necessary
				log.Println("heartbeat context cancelled")
				return
			}
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
			FileName: fileMeta.FileName,
			Chunks:   fileMeta.Chunks,
		})
	}

	var req *api.RegisterRequest
	if c.me == uuid.Nil {
		req = &api.RegisterRequest{
			New:  true,
			Size: float32(c.store.Size()),
			Meta: requestMeta,
		}
	} else {
		req = &api.RegisterRequest{
			New:  false,
			Id:   c.me.String(),
			Size: float32(c.store.Size()),
			Meta: requestMeta,
		}
	}

	stream, err := c.serviceClient.Connect(context.Background(), req)
	if err != nil {
		log.Fatalf("failed to start Connect RPC: %v", err)
		return err
	}

	resp, err := stream.Recv()
	if err != nil {
		log.Printf("failed to receive register ack")
		return err
	}

	if resp.Action != api.FileAction_REGISTER {
		log.Printf("failed to receive register ack action")
		return fmt.Errorf("failed to receive register ack action")
	}

	if c.me == uuid.Nil {
		c.me, err = uuid.Parse(resp.DatanodeId)
		if err != nil {
			log.Printf("failed to parse data node id from master")
			return err
		}
		err := c.store.UpdateMe(c.me)
		if err != nil {
			log.Printf("failed to update me: %v", err)
			return err
		}
	}

	log.Printf("connected to server stream for Data Node ID: %s", req.Id)
	log.Printf("me: %s", c.me.String())

	// register first heart beat now
	wg := &sync.WaitGroup{}
	c.heartBeat(wg)

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				log.Printf("stream context closed: %v", err)
				return err
			}
			if err.Error() == "EOF" {
				log.Println("stream closed by server")
				return err
			}
			log.Fatalf("error receiving from stream: %v", err)
		}

		log.Printf("received response from server: %v", resp)
		switch resp.Action {
		case api.FileAction_UPLOAD_CHUNK:
			err := c.store.Write(resp.FileName, resp.Chunk.ChunkNumber, resp.Chunk.Data)
			if err != nil {
				// failed to write
			}
		}

	}
}
