package memcache

import (
	"time"
)

var DefaultConfig = Config{
	InitConnections:    2,
	MaxIdleConnections: 2,
	ConnectTimeout:     100 * time.Millisecond,
}

type Config struct {
	//Amount of connections which will be dialed during client initialization
	InitConnections int
	//Limit of idle connections, have to be more than amount of inflight requests at the moment
	MaxIdleConnections int
	//Limit of max open connections, if limit exceeded, try to create new connect will fail
	MaxOpenConnections int

	ConnectTimeout time.Duration

	OpenConnectionHook  OpenConnectionHook
	CloseConnectionHook CloseConnectionHook
}
