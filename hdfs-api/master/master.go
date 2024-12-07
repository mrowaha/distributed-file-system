package master

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
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

func (s *HdfsMasterServiceImpl) Serve() {
	s.grpcServer = grpc.NewServer()
	api.RegisterHdfsMasterServiceServer(s.grpcServer, s)
	api.RegisterHdfsDataNodeServiceServer(s.grpcServer, s)
	log.Printf("intial state %v", s.CurrState())
	if err := s.grpcServer.Serve(s.listener); err != nil {
		log.Fatalf("failed to start service %v", err)
	}
}

type StateElement struct {
	FileName    string `json:"file_name"`
	TotalChunks int    `json:"total_chunks"`
}

func (s *HdfsMasterServiceImpl) CurrState() []StateElement {
	data := make(chan interface{})
	go func() {
		s.broker.DispatchEvent(
			data,
			broker.NewBrokerEvent(broker.CurrState),
		)
	}()
	res := <-data
	resStr, ok := res.(string)
	if !ok {
		log.Fatalf("failed to cast initial state")
	}
	var fsState []StateElement
	err := json.Unmarshal([]byte(resStr), &fsState)
	if err != nil {
		log.Fatalf("%v", err)
	}
	return fsState
}

func (s *HdfsMasterServiceImpl) NotifyBroker(event int, data ...interface{}) {
	done := make(chan interface{})
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
	selectedNodeIds := map[int]string{}
	for i := 1; i <= constants.REPLICATION_FACTOR; i++ {
		selectedNodeIds[i] = ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()
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

		fsState := s.CurrState()
		if contains(fsState, req.FileName) {
			return ErrFileAlreadyExists
		}

		for i := 1; i <= constants.REPLICATION_FACTOR; i++ {
			log.Printf("attempting replication %d", i)
			// pre push check to ensure replications are maintained across all nodes
			if s.dataStreamHeap.Len() < constants.REPLICATION_FACTOR {
				log.Printf("no registered data nodes")
				return ErrNotEnoughDataNodes
			}

			var heapElement *DataStreamHeapElement
			if len(selectedNodeIds[i]) == 0 {
				// heap element was not selected, meaning chunk request is the first request
				// and we need to select the heap elements
				heapElement = heap.Pop(s.dataStreamHeap).(*DataStreamHeapElement)
			} else {
				idx := slices.IndexFunc([]*DataStreamHeapElement(*s.dataStreamHeap), func(el *DataStreamHeapElement) bool {
					return el.DataNodeId == selectedNodeIds[i]
				})
				heapElement = []*DataStreamHeapElement(*s.dataStreamHeap)[idx]
				log.Printf("for chunk %d, heap element %s was reused", req.Chunk.ChunkNumber, heapElement.DataNodeId)
			}

			select {
			case heapElement.DataNodeReqChan <- common.MasterAction{
				Action:   api.FileAction_UPLOAD_CHUNK,
				FileName: req.FileName,
				Chunk: common.Chunk{
					FileName:    req.FileName,
					Data:        req.Chunk.Data,
					ChunkNumber: req.Chunk.ChunkNumber,
					TotalChunks: req.TotalChunks,
				},
			}:

				success := <-heapElement.DataNodeResChan
				if success {
					log.Printf("chunk sent to dataNode %s", heapElement.DataNodeId)
					heapElement.LastAccess = time.Now()
					heap.Push(s.dataStreamHeap, heapElement) // updated last access and push the element back
					selectedNodeIds[i] = heapElement.DataNodeId
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
		log.Printf("replicated chunk %d  / %d for file %s", req.Chunk.ChunkNumber, req.TotalChunks, req.FileName)

		if req.Chunk.ChunkNumber == req.TotalChunks {
			// when receiving the first chunk for a file, update namespace
			fileName = req.FileName
			s.NotifyBroker(broker.CreateFile, fileName, int(req.TotalChunks))
			// create file event goes here now since ALL chunks have been successfully
			// REPLICATED. (ALL REPLICATED)
		}

	}
	return nil
}

func (s *HdfsMasterServiceImpl) Heartbeat(ctx context.Context, in *api.HeartBeatRequest) (*api.HeartBeatResponse, error) {
	s.mu.Lock()
	log.Printf("heartbeat from %s, meta: %v", in.Id, in.Meta)
	s.dataStreamHeap.ResetHeartbeat(in.Id)
	s.dataStreamHeap.UpdateMeta(in.Id, in.Meta)
	if s.dataStreamHeap.UpdateSize(in.Id, float64(in.Size)) {
		// log.Printf("node %s updated size to %f", in.Id, in.Size)
	} else {
		// log.Printf("node %s has no size change", in.Id)
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
		DataNodeService:   in.Service,
	})
	s.mu.Unlock()

	log.Printf("appended connection to heap %s, meta: %v", in.Id, in.Meta)
	fsState := s.CurrState()

	fsStateFiles := make([]string, len(fsState))
	for _, fsEl := range fsState {
		fsStateFiles = append(fsStateFiles, fsEl.FileName)
	}
	stream.Send(&api.FileActionResponse{
		Action:     api.FileAction_REGISTER,
		DatanodeId: dataNodeId,
		FsState:    fsStateFiles,
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
					TotalChunks: req.Chunk.TotalChunks,
				},
			}); err != nil {
				log.Printf("failed to send file upload response to data node: %s", dataNodeId)
				resChan <- false
				return err
			}
			resChan <- true
		case <-stream.Context().Done():
			log.Printf("stream canceled for data node: %s", dataNodeId)
			s.mu.Lock()
			idx := slices.IndexFunc([]*DataStreamHeapElement(*s.dataStreamHeap), func(el *DataStreamHeapElement) bool {
				return el.DataNodeId == dataNodeId
			})
			*s.dataStreamHeap = MinHeap(remove([]*DataStreamHeapElement(*s.dataStreamHeap), idx))
			heap.Init(s.dataStreamHeap)
			log.Printf("removed data node %s from heap", dataNodeId)
			s.mu.Unlock()
			return stream.Context().Err()
		}
	}
}

func (s *HdfsMasterServiceImpl) DeleteFile(ctx context.Context, req *api.DeleteFileRequest) (*api.DeleteFileReponse, error) {
	fileName := req.GetFileName()
	log.Printf("received request to delete file: %s", fileName)

	s.NotifyBroker(
		broker.DeleteFile,
		fileName,
	)
	return &api.DeleteFileReponse{Status: true}, nil
}

func (s *HdfsMasterServiceImpl) ReadNodesWithChunks(ctx context.Context, in *api.NodesWithChunksRequest) (*api.NodesWithChunksResponse, error) {
	fileName := in.FileName
	var totalChunks int64

	fsState := s.CurrState()
	for _, fsEl := range fsState {
		if fsEl.FileName == fileName {
			totalChunks = int64(fsEl.TotalChunks)
			break
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// we want to get all the nodes that have these chunks
	heapElements := filter([]*DataStreamHeapElement(*s.dataStreamHeap), func(el *DataStreamHeapElement) bool {
		for _, fileMeta := range el.DataNodeMeta {
			if fileMeta.FileName == fileName {
				return true
			}
		}
		return false
	})

	nodesWithChunks := make([]*api.NodesWithChunksResponse_NodeWithChunk, 0)
	for _, heapEl := range heapElements {
		meta := heapEl.DataNodeMeta

		service := heapEl.DataNodeService
		fileChunks := filter(meta, func(el *api.DataNodeFileMeta) bool {
			return el.FileName == fileName
		})

		chunks := make([]int64, 0)
		for _, fileChunk := range fileChunks {
			chunks = append(chunks, fileChunk.Chunks...)
		}

		nodesWithChunks = append(nodesWithChunks, &api.NodesWithChunksResponse_NodeWithChunk{
			Chunks:  chunks,
			Service: service,
		})
	}
	fmt.Println(nodesWithChunks)

	return &api.NodesWithChunksResponse{
		Nodes:       nodesWithChunks,
		TotalChunks: totalChunks,
	}, nil
}

// helpers //////////////////
func contains(s []StateElement, str string) bool {
	for _, v := range s {
		if v.FileName == str {
			return true
		}
	}

	return false
}

func filter[T any](ss []T, test func(T) bool) (ret []T) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func remove[T any](slice []T, s int) []T {
	return append(slice[:s], slice[s+1:]...)
}
