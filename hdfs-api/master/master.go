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
	"github.com/mrowaha/hdfs-api/common"
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

func (s *HdfsMasterServiceImpl) NotifyBroker(event int, data ...interface{}) {
	done := make(chan struct{})
	go func() {
		s.broker.DispatchEvent(
			done,
			broker.NewBrokerEvent(event, data...),
		)
	}()
	<-done
}

func (s *HdfsMasterServiceImpl) CreateFile(stream grpc.ClientStreamingServer[api.CreateFileRequest, api.CreateFileResponse]) error {
	var fileName string
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Printf("client finished sending chunks")
				break
			}

			// here if the client disconnects without uploading all chunks
			log.Printf("%v", err)
			if len(fileName) != 0 {
				// we had a chunk 1 request
				s.NotifyBroker(broker.DeleteFile, fileName)
			}
			return err
		}

		s.mu.Lock()
		for i := 1; i <= constants.REPLICATION_FACTOR; i++ {
			log.Printf("attempting replication %d", i)
			// pre push check to ensure replications are maintained across all nodes
			if s.dataStreamHeap.Len() < constants.REPLICATION_FACTOR {
				s.mu.Unlock()
				return ErrNotEnoughDataNodes
			}

			heapElement := heap.Pop(s.dataStreamHeap).(*DataStreamHeapElement)

			if time.Since(heapElement.LastHeartBeat) > constants.HEART_BEAT {
				log.Printf("dataNode %s skipped due to stale or missing heartbeat", heapElement.DataNodeId)
				// we can try next iteration
				i--
				continue
			}

			select {
			case heapElement.DataNodeReqChan <- common.MasterAction{
				Action:   api.FileAction_UPLOAD_CHUNK,
				FileName: req.FileName,
				Chunk: common.Chunk{
					FileName:    req.FileName,
					Data:        req.Chunk.Data,
					ChunkNumber: req.Chunk.ChunkNumber,
				},
			}:

				success := <-heapElement.DataNodeResChan
				if success {
					log.Printf("chunk sent to dataNode %s", heapElement.DataNodeId)
					heapElement.LastAccess = time.Now()
					heap.Push(s.dataStreamHeap, heapElement) // updated last access and push the element back
				} else {
					log.Printf("failed to send chunk to dataNode %s", heapElement.DataNodeId)
					i--
				}
			case <-heapElement.DataNodeCloseChan:
				log.Printf("data node closed connection")
				close(heapElement.DataNodeReqChan)
				close(heapElement.DataNodeResChan)
				i--
				// do not push the data node back to heap as the heap has disconnected
			}
		}
		s.mu.Unlock()
		log.Printf("replicated chunk %d  / %d for file %s", req.Chunk.ChunkNumber, req.TotalChunks, req.FileName)

		if req.Chunk.ChunkNumber == req.TotalChunks {
			// when receiving the first chunk for a file, update namespace
			fileName = req.FileName
			s.NotifyBroker(broker.CreateFile, fileName)
			// create file event goes here now since ALL chunks have been successfully
			// REPLICATED. ALL REPLICATED
		}

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

	// associate a channel this node
	reqChan := make(chan common.MasterAction)
	resChan := make(chan bool)
	doneChan := make(chan struct{}, 1)
	s.mu.Lock()
	heap.Push(s.dataStreamHeap, &DataStreamHeapElement{
		DataNodeSize:      float64(in.Size),
		DataNodeId:        dataNodeId,
		DataNodeReqChan:   reqChan,
		DataNodeResChan:   resChan,
		DataNodeCloseChan: doneChan,
		LastHeartBeat:     time.Now(),
		LastAccess:        time.Now(),
		DataNodeMeta:      in.Meta,
	})
	log.Printf("appended connection to heap %s, initial size: %f, meta: %s", in.Id, in.Size, in.Meta)
	s.mu.Unlock()

	stream.Send(&api.FileActionResponse{
		Action:     api.FileAction_REGISTER,
		DatanodeId: dataNodeId,
	})

	// must do close after because lets say a create file is called but the channel is closed
	// before removing the stream
	// then an attempt can be made to write to this channel
	// and that is something we do not want
	// but how can we ensure that the connection is was closed so there is that master
	// maintains the replication factor?
	// easy, do not handle responsibility of closing the channel here

	defer func() {
		doneChan <- struct{}{}
	}()

	for {
		select {
		case req, ok := <-reqChan:
			if !ok {
				log.Printf("request channel closed for data node: %s", dataNodeId)
				return nil
			}
			if err := stream.Send(&api.FileActionResponse{
				Action:   req.Action,
				FileName: req.FileName,
				Chunk: &api.FileChunk{
					Data:        req.Chunk.Data,
					ChunkNumber: req.Chunk.ChunkNumber,
				},
			}); err != nil {
				log.Printf("failed to send file upload response to data node: %s", dataNodeId)
				resChan <- false
				return err
			}
			resChan <- true
		case <-stream.Context().Done():
			log.Printf("stream canceled for data node: %s", dataNodeId)
			return stream.Context().Err()
		}
	}
}
