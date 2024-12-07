package constants

import "time"

const (
	CHUNK_SIZE         = 1024
	REPLICATION_FACTOR = 2
	HEART_BEAT         = 5 * time.Second
)
