package broker

const (
	DeleteFile = iota
	CreateFile
	CurrState
)

type __Broker_Event struct {
	Event string `json:"event"`
}

type __Event_DeleteFile struct {
	__Broker_Event
	FileName string `json:"file_name"`
}

type __Event_CreateFile struct {
	__Broker_Event
	FileName    string `json:"file_name"`
	TotalChunks int    `json:"total_chunks"`
}

func NewBrokerEvent(eventType int, data ...interface{}) interface{} {
	if eventType == DeleteFile {
		return &__Event_DeleteFile{
			FileName:       data[0].(string),
			__Broker_Event: __Broker_Event{Event: "delete_file"},
		}
	} else if eventType == CreateFile {
		return &__Event_CreateFile{
			FileName:       data[0].(string),
			TotalChunks:    data[1].(int),
			__Broker_Event: __Broker_Event{Event: "create_file"},
		}
	} else if eventType == CurrState {
		return &__Broker_Event{
			Event: "curr_state",
		}
	}
	return nil
}

type Broker interface {
	DispatchEvent(chan<- interface{}, interface{}) error
}
