package node

import "sync"

type IHdfsDataNode interface {
	Connect() error
	BeginService(*sync.WaitGroup)
}
