package node

/*
contains definitions for the interface required to store file chunk data
about the data node
*/

import (
	"github.com/google/uuid"
	"github.com/mrowaha/hdfs-api/api"
	common "github.com/mrowaha/hdfs-api/common"
)

type DataNodeFileMeta struct {
	FileName    string
	Chunks      []int64
	TotalChunks int64
}

type DataNodeStore interface {
	Has(fileName string) (chunkNumbers []int64, err error)
	Write(fileName string, chunkNumber int64, totalChunks int64, chunk []byte) error
	Read(fileName string) ([]*common.Chunk, error)
	Me() (uuid.UUID, error)
	UpdateMe(uuid.UUID) error
	Meta() ([]*DataNodeFileMeta, error)
	Size() float64
	ListFiles() ([]string, error)
	DeleteChunks(fileName string) error
	ReadChunks(fileName string, chunks []int64) ([]*api.SendChunkRespose_ChunkData, error)
}
