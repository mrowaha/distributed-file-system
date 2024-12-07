package master

type HdfsMasterService interface {
	Listen(string) error
	Serve()
	NotifyBroker(event int, data ...interface{})
}
