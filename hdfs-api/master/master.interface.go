package master

type HdfsMasterService interface {
	Listen(string) error
	Serve() error
	NotifyBroker(event int, data ...interface{})
}
