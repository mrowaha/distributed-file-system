package master

import (
	"container/heap"
	"fmt"
	"slices"
	"time"

	"github.com/mrowaha/hdfs-api/api"
	"github.com/mrowaha/hdfs-api/common"
)

// Item represents an element in the heap
type DataStreamHeapElement struct {
	DataNodeSize      float64
	DataNodeId        string
	DataNodeReqChan   chan common.MasterAction
	DataNodeCloseChan chan struct{}
	DataNodeResChan   chan bool // if true then it is a success
	LastHeartBeat     time.Time
	LastAccess        time.Time
	DataNodeMeta      []*api.DataNodeFileMeta
}

// MinHeap implements heap.Interface and holds Items
type MinHeap []*DataStreamHeapElement

func (h *MinHeap) ResetHeartbeat(nodeId string) {
	fmt.Println(*h)
	idx := slices.IndexFunc(*h, func(c *DataStreamHeapElement) bool {
		return c.DataNodeId == nodeId
	})
	(*h)[idx].LastHeartBeat = time.Now()
}

func (h *MinHeap) UpdateSize(nodeId string, nodeSize float64) bool {
	idx := slices.IndexFunc(*h, func(c *DataStreamHeapElement) bool {
		return c.DataNodeId == nodeId
	})

	if (*h)[idx].DataNodeSize == nodeSize {
		return false
	}
	(*h)[idx].DataNodeSize = nodeSize
	heap.Fix(h, idx)
	return true
}

func (h *MinHeap) UpdateMeta(nodeId string, meta []*api.DataNodeFileMeta) {
	idx := slices.IndexFunc(*h, func(c *DataStreamHeapElement) bool {
		return c.DataNodeId == nodeId
	})
	(*h)[idx].LastHeartBeat = time.Now()

}

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	return h[i].LastAccess.Before(h[j].LastAccess)
}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push adds an item to the heap
func (h *MinHeap) Push(x interface{}) {
	item := x.(*DataStreamHeapElement)
	*h = append(*h, item)
}

// Pop removes and returns the minimum item from the heap
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1] // Get the last item
	old[n-1] = nil   // Avoid memory leak
	*h = old[0 : n-1]
	return item
}
