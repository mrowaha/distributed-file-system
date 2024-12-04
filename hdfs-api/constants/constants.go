package constants

import "time"

const (
	CHUNK_SIZE         = 1024
	REPLICATION_FACTOR = 3
	HEART_BEAT         = 30 * time.Second
)
