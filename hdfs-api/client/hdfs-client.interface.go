package client

type HdfsClient interface {
	CreateFile(string, string) error
}
