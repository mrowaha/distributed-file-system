package broker

const (
	DeleteFile = iota
)

type __Broker_Event struct {
	Event string `json:"event"`
}

type __Event_DeleteFile struct {
	__Broker_Event
	FileName string `json:"file_name"`
}

func NewBrokerEvent(eventType int, data ...interface{}) interface{} {
	if eventType == DeleteFile {
		return &__Event_DeleteFile{
			FileName:       data[0].(string),
			__Broker_Event: __Broker_Event{Event: "delete_file_chunks"},
		}
	}
	return nil
}

type Broker interface {
	DispatchEvent(interface{}) error
}
