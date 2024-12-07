package master

import "errors"

var ErrNoDataNode = errors.New("no data node connected")
var ErrStaleDataNode = errors.New("stale data node selected")
var ErrNotEnoughDataNodes = errors.New("not enough data nodes are connected for replication")
var ErrFileAlreadyExists = errors.New("file already exists")
