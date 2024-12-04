package master

import (
	"container/heap"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	api "github.com/mrowaha/hdfs-api/api"
	"github.com/mrowaha/hdfs-api/broker"
	common "github.com/mrowaha/hdfs-api/common"
	"github.com/mrowaha/hdfs-api/constants"
	"google.golang.org/grpc"
)

type HdfsMasterServiceImpl struct {
	api.UnimplementedHdfsMasterServiceServer
	api.UnimplementedHdfsDataNodeServiceServer
	listener       net.Listener
	grpcServer     *grpc.Server
	broker         broker.Broker
	dataStreamHeap *MinHeap
	mu             sync.Mutex
}

func NewHdfsMasterServiceImpl(broker broker.Broker) HdfsMasterService {
	dataStreamHeap := &MinHeap{}
	heap.Init(dataStreamHeap)
	return &HdfsMasterServiceImpl{
		broker:         broker,
		dataStreamHeap: dataStreamHeap,
	}
}

func (s *HdfsMasterServiceImpl) Listen(port string) error {
	var err error
	s.listener, err = net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	log.Printf("server listening on port %s", port)
	return nil
}

func (s *HdfsMasterServiceImpl) Serve() error {
	s.grpcServer = grpc.NewServer()
	api.RegisterHdfsMasterServiceServer(s.grpcServer, s)
	api.RegisterHdfsDataNodeServiceServer(s.grpcServer, s)
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.Fatalf("failed to start service %v", err)
	}
	return nil
}

func (s *HdfsMasterServiceImpl) CreateFile(stream grpc.ClientStreamingServer[api.CreateFileRequest, api.CreateFileResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Printf("client finished sending chunks")
				break
			}
			log.Printf("stream ended: %v", err)
			break
		}

		for i := 1; i <= constants.REPLICATION_FACTOR; i++ {
			s.mu.Lock()
			if s.dataStreamHeap.Len() == 0 {
				// we have failed to upload a chunk for a given file
				// but there is a chance that there may be data nodes with this chunk in there
				// so that should be solved
				// solution? use broker to orchestrate this
				// send event for clean up of file across the cluster
				s.broker.DispatchEvent(
					broker.NewBrokerEvent(broker.DeleteFile, req.FileName),
				)
				s.mu.Unlock()
				return ErrNoDataNode
			}

			heapElement := heap.Pop(s.dataStreamHeap).(*DataStreamHeapElement)

			if time.Since(heapElement.LastHeartBeat) > constants.HEART_BEAT {
				log.Printf("dataNode %s skipped due to stale or missing heartbeat", heapElement.DataNodeId)
				// want replication here and remove this node from the heap
				s.mu.Unlock()
				continue
			}

			select {
			case heapElement.DataNodeReqChan <- common.Chunk{
				Data:        req.Chunk.Data,
				ChunkNumber: req.Chunk.ChunkNumber,
				FileName:    req.FileName,
			}:
				log.Printf("chunk sent to dataNode %s", heapElement.DataNodeId)
				heap.Push(s.dataStreamHeap, heapElement) // push the element back
			case <-time.After(1 * time.Second):
				log.Printf("request channel did not extract chunk data")
				close(heapElement.DataNodeReqChan)
				// do not push the data node back to heap as the heap as disconnected
			}
			s.mu.Unlock()
		}
		log.Printf("replicated chunk %d  / %d for file %s", req.Chunk.ChunkNumber, req.TotalChunks, req.FileName)
	}
	return nil
}

func (s *HdfsMasterServiceImpl) Heartbeat(ctx context.Context, in *api.HeartBeatRequest) (*api.HeartBeatResponse, error) {
	s.mu.Lock()
	s.dataStreamHeap.ResetHeartbeat(in.Id)
	s.dataStreamHeap.UpdateMeta(in.Id, in.Meta)
	if s.dataStreamHeap.UpdateSize(in.Id, float64(in.Size)) {
		log.Printf("node %s updated size to %f", in.Id, in.Size)
	} else {
		log.Printf("node %s has no size change", in.Id)
	}
	s.mu.Unlock()

	return &api.HeartBeatResponse{
		Status: api.HeartBeatStatus_OK,
	}, nil
}

func (s *HdfsMasterServiceImpl) Connect(in *api.RegisterRequest, stream grpc.ServerStreamingServer[api.FileActionResponse]) error {
	var dataNodeId string
	if in.New {
		dataNodeUuidId, _ := uuid.NewRandom()
		dataNodeId = dataNodeUuidId.String()
	} else {
		dataNodeId = in.Id
	}

	stream.Send(&api.FileActionResponse{
		Action:     api.FileAction_REGISTER,
		DatanodeId: dataNodeId,
	})

	// associate a channel this node
	reqChan := make(chan common.Chunk)
	s.mu.Lock()
	heap.Push(s.dataStreamHeap, &DataStreamHeapElement{
		DataNodeSize:    float64(in.Size),
		DataNodeId:      in.Id,
		DataNodeReqChan: reqChan,
		LastHeartBeat:   time.Now(),
		DataNodeMeta:    in.Meta,
	})
	log.Printf("appended connection to heap %s, initial size: %f, meta: %s", in.Id, in.Size, in.Meta)
	s.mu.Unlock()

	// must do close after because lets say a create file is called but the channel is closed
	// before removing the stream
	// then an attempt can be made to write to this channel
	// and that is something we do not want
	// but how can we ensure that the connection is was closed so there is that master
	// maintains the replication factor?
	// easy, do not handle responsibility of closing the channel here

	for {
		select {
		case req, ok := <-reqChan:
			if !ok {
				log.Printf("request channel closed for data node: %s", dataNodeId)
				return nil
			}
			if err := stream.Send(&api.FileActionResponse{
				Action:   api.FileAction_UPLOAD_CHUNK,
				FileName: req.FileName,
				Chunk: &api.FileChunk{
					Data:        req.Data,
					ChunkNumber: req.ChunkNumber,
				},
			}); err != nil {
				log.Printf("failed to send file upload response to data node: %s", dataNodeId)
				return err
			}
		case <-stream.Context().Done():
			log.Printf("stream canceled for data node: %s", dataNodeId)
			return stream.Context().Err()
		}
	}
}
