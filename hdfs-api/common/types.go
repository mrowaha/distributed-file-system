package common

import "github.com/mrowaha/hdfs-api/api"

type Chunk struct {
	FileName    string
	Data        []byte
	ChunkNumber int64
	TotalChunks int64
}

type MasterAction struct {
	Action   api.FileAction
	FileName string
	Chunk    Chunk
}
