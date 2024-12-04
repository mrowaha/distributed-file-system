package master

type HdfsMasterService interface {
	Listen(string) error
	Serve() error
}
