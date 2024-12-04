package common

type Chunk struct {
	FileName    string
	Data        []byte
	ChunkNumber int64
}
