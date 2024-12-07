package client

import "github.com/mrowaha/hdfs-api/api"

type HdfsClient interface {
	CreateFile(string, string) error
	DeleteFile(string) error
	FetchNodes(string) (*api.NodesWithChunksResponse, error)
	GetChunksFromNodes(string, int, []*api.NodesWithChunksResponse_NodeWithChunk)
}
